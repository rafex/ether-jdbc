package dev.rafex.ether.jdbc.datasource;

/*-
 * #%L
 * ether-jdbc
 * %%
 * Copyright (C) 2025 - 2026 Raúl Eduardo González Argote
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * #L%
 */

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * A simple implementation of {@link DataSource} that uses {@link DriverManager} to create connections.
 *
 * <p>This implementation is suitable for applications that need a straightforward way to obtain
 * database connections without complex connection pooling.</p>
 */
public final class SimpleDataSource implements DataSource {

    private final String url;
    private final Properties properties;
    private volatile PrintWriter logWriter;
    private volatile int loginTimeout;

    /**
     * Creates a new {@code SimpleDataSource} with the specified JDBC URL.
     *
     * @param url the JDBC URL of the database
     * @throws NullPointerException if {@code url} is {@code null}
     */
    public SimpleDataSource(final String url) {
        this(url, new Properties());
    }

    /**
     * Creates a new {@code SimpleDataSource} with the specified JDBC URL, username, and password.
     *
     * @param url      the JDBC URL of the database
     * @param username the database username
     * @param password the database password
     * @throws NullPointerException if {@code url} is {@code null}
     */
    public SimpleDataSource(final String url, final String username, final String password) {
        this(url, toProperties(username, password));
    }

    /**
     * Creates a new {@code SimpleDataSource} with the specified JDBC URL and properties.
     *
     * @param url        the JDBC URL of the database
     * @param properties the connection properties
     * @throws NullPointerException if {@code url} is {@code null}
     */
    public SimpleDataSource(final String url, final Properties properties) {
        this.url = Objects.requireNonNull(url, "url");
        this.properties = new Properties();
        if (properties != null) {
            this.properties.putAll(properties);
        }
    }

    /**
     * Retrieves a database connection using the configured URL and properties.
     *
     * @return a database connection
     * @throws SQLException if a database access error occurs
     */
    @Override
    public Connection getConnection() throws SQLException {
        DriverManager.setLoginTimeout(loginTimeout);
        if (logWriter != null) {
            DriverManager.setLogWriter(logWriter);
        }
        return DriverManager.getConnection(url, properties);
    }

    /**
     * Retrieves a database connection using the specified username and password.
     *
     * @param username the database username
     * @param password the database password
     * @return a database connection
     * @throws SQLException if a database access error occurs
     */
    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    @Override
    public void setLogWriter(final PrintWriter out) {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(final int seconds) {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() {
        return loginTimeout;
    }

    /**
     * Not supported by this implementation.
     *
     * @return nothing, always throws an exception
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Parent logger is not supported");
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Unsupported unwrap type: " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return iface.isInstance(this);
    }

    private static Properties toProperties(final String username, final String password) {
        final var properties = new Properties();
        if (username != null) {
            properties.setProperty("user", username);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }
        return properties;
    }
}
