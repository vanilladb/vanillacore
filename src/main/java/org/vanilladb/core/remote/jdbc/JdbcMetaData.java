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

import java.sql.SQLException;

/**
 * An adapter class that wraps RemoteMetaData. Its methods do nothing except
 * transform RemoteExceptions into SQLExceptions.
 */
public class JdbcMetaData extends ResultSetMetaDataAdapter {
	private RemoteMetaData rmd;

	public JdbcMetaData(RemoteMetaData md) {
		rmd = md;
	}

	public int getColumnCount() throws SQLException {
		try {
			return rmd.getColumnCount();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public String getColumnName(int column) throws SQLException {
		try {
			return rmd.getColumnName(column);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public int getColumnType(int column) throws SQLException {
		try {
			return rmd.getColumnType(column);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public int getColumnDisplaySize(int column) throws SQLException {
		try {
			return rmd.getColumnDisplaySize(column);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
