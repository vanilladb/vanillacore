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

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * The VanillaDb database driver for JDBC.
 */
public class JdbcDriver extends DriverAdapter {

	/**
	 * Connects to the VanillaDb server on the specified host. The method
	 * retrieves the RemoteDriver stub from the RMI registry on the specified
	 * host. It then calls the connect method on that stub, which in turn
	 * creates a new connection and returns the RemoteConnection stub for it.
	 * This stub is wrapped in a {@link JdbcConnection} object and is returned.
	 * <P>
	 * The current implementation of this method ignores the properties
	 * argument.
	 * 
	 * @see java.sql.Driver#connect(java.lang.String, Properties)
	 */
	public Connection connect(String url, Properties prop) throws SQLException {
		try {
			// assumes no port specified
			String host = url.replace("jdbc:vanilladb://", "");
			Registry reg = LocateRegistry.getRegistry(host);
			RemoteDriver rdvr = (RemoteDriver) reg.lookup(JdbcStartUp.RMI_REG_NAME);
			RemoteConnection rconn = rdvr.connect();
			return new JdbcConnection(rconn);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
