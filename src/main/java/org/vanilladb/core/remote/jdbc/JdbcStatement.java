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

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * An adapter class that wraps RemoteStatement. Its methods do nothing except
 * transform RemoteExceptions into SQLExceptions.
 */
public class JdbcStatement extends StatementAdapter {
	private RemoteStatement rstmt;

	public JdbcStatement(RemoteStatement s) {
		rstmt = s;
	}

	public ResultSet executeQuery(String qry) throws SQLException {
		try {
			RemoteResultSet rrs = rstmt.executeQuery(qry);
			return new JdbcResultSet(rrs);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public int executeUpdate(String cmd) throws SQLException {
		try {
			return rstmt.executeUpdate(cmd);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

}
