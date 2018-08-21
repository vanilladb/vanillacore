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
 * An adapter class that wraps RemoteResultSet. Its methods do nothing except
 * transform RemoteExceptions into SQLExceptions.
 */
public class JdbcResultSet extends ResultSetAdapter {
	private RemoteResultSet rrs;

	public JdbcResultSet(RemoteResultSet s) {
		rrs = s;
	}

	public boolean next() throws SQLException {
		try {
			return rrs.next();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public int getInt(String fldName) throws SQLException {
		try {
			return rrs.getInt(fldName);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public long getLong(String fldName) throws SQLException {
		try {
			return rrs.getLong(fldName);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public double getDouble(String fldName) throws SQLException {
		try {
			return rrs.getDouble(fldName);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public String getString(String fldName) throws SQLException {
		try {
			return rrs.getString(fldName);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		try {
			RemoteMetaData rmd = rrs.getMetaData();
			return new JdbcMetaData(rmd);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public void close() throws SQLException {
		try {
			rrs.close();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void beforeFirst() throws SQLException {
		try {
			rrs.beforeFirst();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
