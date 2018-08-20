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

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The RMI remote interface corresponding to Statement. The methods are
 * identical to those of Statement, except that they throw RemoteExceptions
 * instead of SQLExceptions.
 */
public interface RemoteStatement extends Remote {

	RemoteResultSet executeQuery(String qry) throws RemoteException;

	int executeUpdate(String cmd) throws RemoteException;
}
