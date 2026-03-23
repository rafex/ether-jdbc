package dev.rafex.ether.jdbc.client;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import dev.rafex.ether.database.core.exceptions.DatabaseAccessException;
import dev.rafex.ether.database.core.DatabaseClient;
import dev.rafex.ether.database.core.mapping.ResultSetExtractor;
import dev.rafex.ether.database.core.mapping.RowMapper;
import dev.rafex.ether.database.core.sql.SqlParameter;
import dev.rafex.ether.database.core.sql.SqlQuery;
import dev.rafex.ether.database.core.sql.StatementBinder;
import dev.rafex.ether.database.core.transaction.TransactionCallback;

public final class JdbcDatabaseClient implements DatabaseClient {

    private final DataSource dataSource;

    public JdbcDatabaseClient(final DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public <T> T query(final SqlQuery query, final ResultSetExtractor<T> extractor) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(query.sql())) {
            bind(statement, connection, query.parameters());
            try (var resultSet = statement.executeQuery()) {
                return extractor.extract(resultSet);
            }
        } catch (SQLException e) {
            throw new DatabaseAccessException("Query failed", e);
        }
    }

    @Override
    public <T> List<T> queryList(final SqlQuery query, final RowMapper<T> mapper) {
        return query(query, resultSet -> {
            final List<T> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(mapper.map(resultSet));
            }
            return List.copyOf(result);
        });
    }

    @Override
    public <T> Optional<T> queryOne(final SqlQuery query, final RowMapper<T> mapper) {
        return query(query, resultSet -> {
            if (!resultSet.next()) {
                return Optional.empty();
            }
            final T result = mapper.map(resultSet);
            if (resultSet.next()) {
                throw new SQLException("Expected at most one row");
            }
            return Optional.of(result);
        });
    }

    @Override
    public int execute(final SqlQuery query) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(query.sql())) {
            bind(statement, connection, query.parameters());
            statement.execute();
            return statement.getUpdateCount();
        } catch (SQLException e) {
            throw new DatabaseAccessException("Execute failed", e);
        }
    }

    @Override
    public long[] batch(final String sql, final List<StatementBinder> binders) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            for (final StatementBinder binder : binders) {
                binder.bind(connection, statement);
                statement.addBatch();
            }
            return statement.executeLargeBatch();
        } catch (SQLException e) {
            throw new DatabaseAccessException("Batch execution failed", e);
        }
    }

    @Override
    public <T> T inTransaction(final TransactionCallback<T> callback) {
        try (Connection connection = dataSource.getConnection()) {
            final boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                final T result = callback.execute(connection);
                connection.commit();
                connection.setAutoCommit(previousAutoCommit);
                return result;
            } catch (Exception e) {
                connection.rollback();
                connection.setAutoCommit(previousAutoCommit);
                throw wrap("Transaction failed", e);
            }
        } catch (SQLException e) {
            throw new DatabaseAccessException("Transaction setup failed", e);
        }
    }

    private static void bind(final PreparedStatement statement, final Connection connection,
            final List<SqlParameter> parameters) throws SQLException {
        for (int i = 0; i < parameters.size(); i++) {
            final SqlParameter parameter = parameters.get(i);
            final int index = i + 1;
            if (parameter.arrayElementType() != null) {
                if (parameter.value() == null) {
                    statement.setNull(index, parameter.sqlType() == null ? java.sql.Types.ARRAY : parameter.sqlType());
                } else {
                    statement.setArray(index,
                            connection.createArrayOf(parameter.arrayElementType(), (Object[]) parameter.value()));
                }
                continue;
            }
            if (parameter.value() == null) {
                statement.setNull(index, parameter.sqlType() == null ? java.sql.Types.NULL : parameter.sqlType());
                continue;
            }
            if (parameter.sqlType() != null) {
                statement.setObject(index, parameter.value(), parameter.sqlType());
                continue;
            }
            statement.setObject(index, parameter.value());
        }
    }

    private static RuntimeException wrap(final String message, final Exception exception) {
        return switch (exception) {
            case RuntimeException re -> re;
            case SQLException se     -> new DatabaseAccessException(message, se);
            default                  -> new DatabaseAccessException(message, exception);
        };
    }
}
