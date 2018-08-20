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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * This class implements all of the methods of the ResultSetMetaData interface,
 * by throwing an exception for each one. Subclasses (such as
 * {@link JdbcMetaData}) can override those methods that it want to implement.
 */
public abstract class ResultSetMetaDataAdapter implements ResultSetMetaData {
	@Override
	public String getCatalogName(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getColumnCount() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getScale(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getTableName(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int isNullable(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
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
}
