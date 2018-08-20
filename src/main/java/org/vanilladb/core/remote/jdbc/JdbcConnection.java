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
import java.sql.Statement;

/**
 * An adapter class that wraps RemoteConnection. Its methods do nothing except
 * transform RemoteExceptions into SQLExceptions.
 */
public class JdbcConnection extends ConnectionAdapter {
	private RemoteConnection rconn;

	public JdbcConnection(RemoteConnection c) {
		rconn = c;
	}

	public Statement createStatement() throws SQLException {
		try {
			RemoteStatement rstmt = rconn.createStatement();
			return new JdbcStatement(rstmt);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public void close() throws SQLException {
		try {
			rconn.close();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		try {
			rconn.setAutoCommit(autoCommit);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		try {
			rconn.setReadOnly(readOnly);
		} catch (Exception e) {
			throw new SQLException(e);
		}

	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		try {
			rconn.setTransactionIsolation(level);
		} catch (Exception e) {
			throw new SQLException(e);
		}

	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		try {
			return rconn.getAutoCommit();
		} catch (Exception e) {
			throw new SQLException(e);
		}

	}

	@Override
	public boolean isReadOnly() throws SQLException {
		try {
			return rconn.isReadOnly();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		try {
			return rconn.getTransactionIsolation();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void commit() throws SQLException {
		try {
			rconn.commit();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void rollback() throws SQLException {
		try {
			rconn.rollback();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
