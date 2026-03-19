package dev.rafex.ether.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import dev.rafex.ether.database.core.SqlParameter;
import dev.rafex.ether.database.core.SqlQuery;

class JdbcDatabaseClientTest {

	@Test
	void shouldQueryOneAndExecuteBatch() {
		final var resultSet = resultSet(List.of(Map.of("id", 7, "name", "db")));
		final var preparedStatement = preparedStatement(resultSet, 3);
		final var dataSource = dataSource(connection(preparedStatement));
		final var client = new JdbcDatabaseClient(dataSource);

		final Optional<String> value = client.queryOne(SqlQuery.of("select * from demo"), rs -> rs.getString("name"));
		final int updated = client.execute(new SqlQuery("update demo set name = ?", List.of(SqlParameter.of("next"))));
		final long[] batch = client.batch("insert into demo(name) values (?)",
				List.of((conn, ps) -> ps.setString(1, "a"), (conn, ps) -> ps.setString(1, "b")));

		assertTrue(value.isPresent());
		assertEquals("db", value.orElseThrow());
		assertEquals(3, updated);
		assertArrayEquals(new long[] { 1L, 1L }, batch);
	}

	@Test
	void shouldCommitTransaction() {
		final var commits = new AtomicInteger();
		final var rollbacks = new AtomicInteger();
		final var preparedStatement = preparedStatement(resultSet(List.of()), 0);
		final var client = new JdbcDatabaseClient(dataSource(connection(preparedStatement, commits, rollbacks)));

		final var result = client.inTransaction(connection -> "ok");

		assertEquals("ok", result);
		assertEquals(1, commits.get());
		assertEquals(0, rollbacks.get());
	}

	private static DataSource dataSource(final Connection connection) {
		return (DataSource) Proxy.newProxyInstance(
				DataSource.class.getClassLoader(),
				new Class<?>[] { DataSource.class },
				(proxy, method, args) -> switch (method.getName()) {
					case "getConnection" -> connection;
					case "isWrapperFor" -> false;
					case "unwrap" -> throw new SQLException("Unsupported");
					default -> defaultValue(method.getReturnType());
				});
	}

	private static Connection connection(final PreparedStatement preparedStatement) {
		return connection(preparedStatement, new AtomicInteger(), new AtomicInteger());
	}

	private static Connection connection(final PreparedStatement preparedStatement, final AtomicInteger commits,
			final AtomicInteger rollbacks) {
		final InvocationHandler handler = new InvocationHandler() {
			private boolean autoCommit = true;

			@Override
			public Object invoke(final Object proxy, final java.lang.reflect.Method method, final Object[] args)
					throws Throwable {
				return switch (method.getName()) {
					case "prepareStatement" -> preparedStatement;
					case "getAutoCommit" -> autoCommit;
					case "setAutoCommit" -> {
						autoCommit = (Boolean) args[0];
						yield null;
					}
					case "commit" -> {
						commits.incrementAndGet();
						yield null;
					}
					case "rollback" -> {
						rollbacks.incrementAndGet();
						yield null;
					}
					case "createArrayOf" -> array((Object[]) args[1]);
					case "close" -> null;
					case "isWrapperFor" -> false;
					case "unwrap" -> throw new SQLException("Unsupported");
					default -> defaultValue(method.getReturnType());
				};
			}
		};
		return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
				new Class<?>[] { Connection.class }, handler);
	}

	private static PreparedStatement preparedStatement(final ResultSet resultSet, final int updateCount) {
		final InvocationHandler handler = new InvocationHandler() {
			private int batchCount;

			@Override
			public Object invoke(final Object proxy, final java.lang.reflect.Method method, final Object[] args)
					throws Throwable {
				return switch (method.getName()) {
					case "executeQuery" -> resultSet;
					case "executeUpdate" -> updateCount;
					case "addBatch" -> {
						batchCount++;
						yield null;
					}
					case "executeLargeBatch" -> {
						final long[] out = new long[batchCount];
						java.util.Arrays.fill(out, 1L);
						yield out;
					}
					case "close", "setObject", "setString", "setNull", "setArray" -> null;
					case "isWrapperFor" -> false;
					case "unwrap" -> throw new SQLException("Unsupported");
					default -> defaultValue(method.getReturnType());
				};
			}
		};
		return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
				new Class<?>[] { PreparedStatement.class }, handler);
	}

	private static ResultSet resultSet(final List<Map<String, Object>> rows) {
		final InvocationHandler handler = new InvocationHandler() {
			private int index = -1;

			@Override
			public Object invoke(final Object proxy, final java.lang.reflect.Method method, final Object[] args)
					throws Throwable {
				return switch (method.getName()) {
					case "next" -> ++index < rows.size();
					case "getString" -> String.valueOf(rows.get(index).get(args[0]));
					case "getInt" -> ((Number) rows.get(index).get(args[0])).intValue();
					case "getObject" -> rows.get(index).get(args[0]);
					case "close" -> null;
					case "isWrapperFor" -> false;
					case "unwrap" -> throw new SQLException("Unsupported");
					default -> defaultValue(method.getReturnType());
				};
			}
		};
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
				new Class<?>[] { ResultSet.class }, handler);
	}

	private static Array array(final Object[] values) {
		return (Array) Proxy.newProxyInstance(Array.class.getClassLoader(), new Class<?>[] { Array.class },
				(proxy, method, args) -> switch (method.getName()) {
					case "getArray" -> values;
					case "free" -> null;
					case "isWrapperFor" -> false;
					case "unwrap" -> throw new SQLException("Unsupported");
					default -> defaultValue(method.getReturnType());
				});
	}

	private static Object defaultValue(final Class<?> type) {
		if (type == boolean.class) {
			return false;
		}
		if (type == int.class) {
			return 0;
		}
		if (type == long.class) {
			return 0L;
		}
		return null;
	}
}
