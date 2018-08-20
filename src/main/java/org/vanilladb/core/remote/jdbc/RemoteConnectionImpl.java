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

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

/**
 * The RMI server-side implementation of RemoteConnection.
 */
@SuppressWarnings("serial")
class RemoteConnectionImpl extends UnicastRemoteObject implements
		RemoteConnection {
	private static final int DEFAULT_ISOLATION_LEVEL;
	private Transaction tx;
	private boolean autoCommit = true;
	private boolean readOnly = false;
	private int isolationLevel = DEFAULT_ISOLATION_LEVEL;

	static {
		DEFAULT_ISOLATION_LEVEL = CoreProperties.getLoader().getPropertyAsInteger(
				RemoteConnectionImpl.class.getName() + ".DEFAULT_ISOLATION_LEVEL",
				Connection.TRANSACTION_SERIALIZABLE);
	}

	/**
	 * Creates a remote connection and begins a new transaction for it.
	 * 
	 * @throws RemoteException
	 */
	RemoteConnectionImpl() throws RemoteException {
		try {
			tx = VanillaDb.txMgr().newTransaction(isolationLevel, readOnly);
		} catch (Exception e) {
			throw new RemoteException("error creating transaction ", e);
		}
	}

	/**
	 * Creates a new RemoteStatement for this connection.
	 * 
	 * @see RemoteConnection#createStatement()
	 */
	@Override
	public RemoteStatement createStatement() throws RemoteException {
		return new RemoteStatementImpl(this);
	}

	/**
	 * Closes the connection. The current transaction is committed.
	 * 
	 * @see RemoteConnection#close()
	 */
	@Override
	public void close() throws RemoteException {
		tx.commit();
	}

	/**
	 * Sets this connection's auto-commit mode to the given state. The default
	 * setting of auto-commit mode is true.
	 */
	@Override
	public void setAutoCommit(boolean autoCommit) throws RemoteException {
		this.autoCommit = autoCommit;
	}

	/**
	 * Sets this connection's auto-commit mode to the given state. The default
	 * setting of auto-commit mode is true. This method may commit current
	 * transaction and start a new transaction.
	 */
	@Override
	public void setReadOnly(boolean readOnly) throws RemoteException {
		if (this.readOnly != readOnly) {
			tx.commit();
			this.readOnly = readOnly;
			try {
				tx = VanillaDb.txMgr().newTransaction(isolationLevel, readOnly);
			} catch (Exception e) {
				throw new RemoteException("error creating transaction ", e);
			}
		}
	}

	/**
	 * Attempts to change the transaction isolation level for this connection.
	 * This method will commit current transaction and start a new transaction.
	 * Currently, the only supported isolation levels are
	 * {@link Connection#TRANSACTION_SERIALIZABLE} and
	 * {@link Connection#TRANSACTION_REPEATABLE_READ}. The auto-commit mode must
	 * be set to false before calling this method.
	 */
	@Override
	public void setTransactionIsolation(int level) throws RemoteException {
		if (getAutoCommit() != false)
			throw new RemoteException(
					"the auto-commit mode need to be set to false before changing the isolation level");
		if (this.isolationLevel == level)
			return;
		tx.commit();
		this.isolationLevel = level;
		try {
			tx = VanillaDb.txMgr().newTransaction(isolationLevel, readOnly);
		} catch (Exception e) {
			throw new RemoteException("error creating transaction ", e);
		}
	}

	/**
	 * Retrieves the current auto-commit mode for this Connection object. *
	 */
	@Override
	public boolean getAutoCommit() throws RemoteException {
		return this.autoCommit;
	}

	/**
	 * Retrieves whether this Connection object is in read-only mode.
	 */
	@Override
	public boolean isReadOnly() throws RemoteException {
		return this.readOnly;
	}

	/**
	 * Retrieves this Connection object's current transaction isolation level.
	 */
	@Override
	public int getTransactionIsolation() throws RemoteException {
		return this.isolationLevel;

	}

	/**
	 * Commits the current transaction, and begins a new one.
	 * 
	 * @throws Exception
	 */
	@Override
	public void commit() throws RemoteException {
		tx.commit();
		try {
			tx = VanillaDb.txMgr().newTransaction(isolationLevel, readOnly);
		} catch (Exception e) {
			throw new RemoteException("error creating transaction ", e);
		}
	}

	/**
	 * Rolls back the current transaction, and begins a new one.
	 * 
	 * @throws Exception
	 */
	@Override
	public void rollback() throws RemoteException {
		tx.rollback();
		try {
			tx = VanillaDb.txMgr().newTransaction(isolationLevel, readOnly);
		} catch (Exception e) {
			throw new RemoteException("error creating transaction ", e);
		}
	}

	// The following methods are used by the server-side classes.

	/**
	 * Returns the transaction currently associated with this connection.
	 * 
	 * @return the transaction associated with this connection
	 */
	Transaction getTransaction() throws RemoteException {
		return tx;
	}

	/**
	 * Finish the current statement.
	 */
	void endStatement() {
		tx.endStatement();
	}
}
