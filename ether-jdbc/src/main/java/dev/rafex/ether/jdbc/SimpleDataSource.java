package dev.rafex.ether.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

public final class SimpleDataSource implements DataSource {

	private final String url;
	private final Properties properties;
	private volatile PrintWriter logWriter;
	private volatile int loginTimeout;

	public SimpleDataSource(final String url) {
		this(url, new Properties());
	}

	public SimpleDataSource(final String url, final String username, final String password) {
		this(url, toProperties(username, password));
	}

	public SimpleDataSource(final String url, final Properties properties) {
		this.url = Objects.requireNonNull(url, "url");
		this.properties = new Properties();
		if (properties != null) {
			this.properties.putAll(properties);
		}
	}

	@Override
	public Connection getConnection() throws SQLException {
		DriverManager.setLoginTimeout(loginTimeout);
		if (logWriter != null) {
			DriverManager.setLogWriter(logWriter);
		}
		return DriverManager.getConnection(url, properties);
	}

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
