/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.remote.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * This class implements all of the methods of the Connection interface, by
 * throwing an exception for each one. Subclasses (such as
 * {@link JdbcConnection}) can override those methods that it want to implement.
 */
public abstract class ConnectionAdapter implements Connection {
	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void close() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Statement createStatement() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getCatalog() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isClosed() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void rollback(Savepoint svepoint) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void setClientInfo(String name, String value) {
	}

	@Override
	public void setClientInfo(Properties properties) {
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getSchema() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		throw new SQLException("operation not implemented");

	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		throw new SQLException("operation not implemented");

	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		throw new SQLException("operation not implemented");
	}
}
