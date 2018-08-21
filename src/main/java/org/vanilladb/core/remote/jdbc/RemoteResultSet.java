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
 * The RMI remote interface corresponding to ResultSet. The methods are
 * identical to those of ResultSet, except that they throw RemoteExceptions
 * instead of SQLExceptions.
 */
public interface RemoteResultSet extends Remote {

	boolean next() throws RemoteException;

	int getInt(String fldName) throws RemoteException;

	long getLong(String fldName) throws RemoteException;

	double getDouble(String fldName) throws RemoteException;

	String getString(String fldName) throws RemoteException;

	RemoteMetaData getMetaData() throws RemoteException;

	void close() throws RemoteException;

	void beforeFirst() throws RemoteException;
}
