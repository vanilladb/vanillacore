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

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.DOUBLE;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Schema;

/**
 * The RMI server-side implementation of RemoteResultSet.
 */
@SuppressWarnings("serial")
class RemoteResultSetImpl extends UnicastRemoteObject implements
		RemoteResultSet {
	private Scan s;
	private Schema schema;
	private RemoteConnectionImpl rconn;

	/**
	 * Creates a RemoteResultSet object. The specified plan is opened, and the
	 * scan is saved.
	 * 
	 * @param plan
	 *            the query plan
	 * @param rconn
	 * 
	 * @throws RemoteException
	 */
	public RemoteResultSetImpl(Plan plan, RemoteConnectionImpl rconn)
			throws RemoteException {
		s = plan.open();
		schema = plan.schema();
		this.rconn = rconn;
	}

	/**
	 * Moves to the next record in the result set, by moving to the next record
	 * in the saved scan.
	 * 
	 * @see org.vanilladb.core.remote.jdbc.RemoteResultSet#next()
	 */
	@Override
	public boolean next() throws RemoteException {
		try {
			return s.next();
		} catch (RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}

	/**
	 * Returns the integer value of the specified field, by returning the
	 * corresponding value on the saved scan.
	 * 
	 * @see org.vanilladb.core.remote.jdbc.RemoteResultSet#getInt(java.lang.String)
	 */
	@Override
	public int getInt(String fldName) throws RemoteException {
		try {
			fldName = fldName.toLowerCase(); // to ensure case-insensitivity
			return (Integer) s.getVal(fldName).castTo(INTEGER).asJavaVal();
		} catch (RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}

	/**
	 * Returns the long value of the specified field, by returning the
	 * corresponding value on the saved scan.
	 * 
	 * @see org.vanilladb.core.remote.jdbc.RemoteResultSet#getLong(java.lang.String)
	 */
	@Override
	public long getLong(String fldName) throws RemoteException {
		try {
			fldName = fldName.toLowerCase(); // to ensure case-insensitivity
			return (Long) s.getVal(fldName).castTo(BIGINT).asJavaVal();
		} catch (RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}

	/**
	 * Returns the double value of the specified field, by returning the
	 * corresponding value on the saved scan.
	 * 
	 * @see org.vanilladb.core.remote.jdbc.RemoteResultSet#getDouble(java.lang.String)
	 */
	@Override
	public double getDouble(String fldName) throws RemoteException {
		try {
			fldName = fldName.toLowerCase(); // to ensure case-insensitivity
			return (Double) s.getVal(fldName).castTo(DOUBLE).asJavaVal();
		} catch (RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}

	/**
	 * Returns the string value of the specified field, by returning the
	 * corresponding value on the saved scan.
	 * 
	 * @see org.vanilladb.core.remote.jdbc.RemoteResultSet#getInt(java.lang.String)
	 */
	@Override
	public String getString(String fldName) throws RemoteException {
		try {
			fldName = fldName.toLowerCase(); // to ensure case-insensitivity
			return (String) s.getVal(fldName).castTo(VARCHAR).asJavaVal();
		} catch (RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}

	/**
	 * Returns the result set's metadata, by passing its schema into the
	 * RemoteMetaData constructor.
	 * 
	 * @see org.vanilladb.core.remote.jdbc.RemoteResultSet#getMetaData()
	 */
	@Override
	public RemoteMetaData getMetaData() throws RemoteException {
		return new RemoteMetaDataImpl(schema);
	}

	/**
	 * Closes the result set by closing its scan.
	 * 
	 * @see org.vanilladb.core.remote.jdbc.RemoteResultSet#close()
	 */
	@Override
	public void close() throws RemoteException {
		s.close();
		if (rconn.getAutoCommit())
			rconn.commit();
		else
			rconn.endStatement();
	}

	/**
	 * Positions the scan before its first record.
	 */
	@Override
	public void beforeFirst() throws RemoteException {
		s.beforeFirst();
	}
}
