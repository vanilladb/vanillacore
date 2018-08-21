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
package org.vanilladb.core.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.vanilladb.core.remote.jdbc.JdbcDriver;

public class ConsoleSQLInterpreter {
	private static Connection conn = null;

	public static void main(String[] args) {
		try {
			Driver d = new JdbcDriver();
			conn = d.connect("jdbc:vanilladb://localhost", null);

			Reader rdr = new InputStreamReader(System.in);
			BufferedReader br = new BufferedReader(rdr);

			while (true) {
				// process one line of input
				System.out.print("\nSQL> ");
				String cmd = br.readLine().trim();
				System.out.println();

				String [] str = cmd.split(" ");
				String cmdf = str[0].toUpperCase();	

				if (cmd.startsWith("exit") || cmd.startsWith("EXIT"))
					break;
				else if (cmdf.startsWith("SELECT")|| cmdf.startsWith("EXPLAIN"))
					doQuery(cmd,cmdf);
				else
					doUpdate(cmd);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void doQuery(String cmd,String cmdf) {
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(cmd);
			ResultSetMetaData md = rs.getMetaData();
			int numcols = md.getColumnCount();
			int totalwidth = 0;

			// print header
			for (int i = 1; i <= numcols; i++) {
				int width = md.getColumnDisplaySize(i);
				totalwidth += width;
				String fmt = "%" + width + "s";
				if (cmdf.startsWith("EXPLAIN"))
					System.out.format("%s", md.getColumnName(i));
				else
					System.out.format(fmt, md.getColumnName(i));
			}

			System.out.println();
			for (int i = 0; i < totalwidth; i++)
				System.out.print("-");
			if (!cmdf.startsWith("EXPLAIN"))
				System.out.println();

			rs.beforeFirst();
			// print records
			while (rs.next()) {
				for (int i = 1; i <= numcols; i++) {
					String fldname = md.getColumnName(i);
					int fldtype = md.getColumnType(i);
					String fmt = "%" + md.getColumnDisplaySize(i);
					if (fldtype == Types.INTEGER)
						System.out.format(fmt + "d", rs.getInt(fldname));
					else if (fldtype == Types.BIGINT)
						System.out.format(fmt + "d", rs.getLong(fldname));
					else if (fldtype == Types.DOUBLE)
						System.out.format(fmt + "f", rs.getDouble(fldname));
					else
						System.out.format(fmt + "s", rs.getString(fldname));
				}
				System.out.println();
			}
			rs.close();
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}

	private static void doUpdate(String cmd) {
		try {
			Statement stmt = conn.createStatement();
			int howmany = stmt.executeUpdate(cmd);
			System.out.println(howmany + " records processed");
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
