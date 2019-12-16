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
package org.vanilladb.core.storage.tx;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.ReadCommittedConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.RepeatableReadConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.SerializableConcurrencyMgr;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.core.util.CoreProperties;

/**
 * The publicly-accessible transaction manager in VanillaDb. This transaction
 * manager is responsible for creating new transaction and maintaining the
 * active transaction list.
 */
public class TransactionMgr implements TransactionLifecycleListener {
	private static Logger logger = Logger.getLogger(TransactionMgr.class.getName());
	public static Class<?> serialConcurMgrCls, rrConcurMgrCls, rcConcurMgrCls, recoveryMgrCls, bufferMgrCls;
	static {
		serialConcurMgrCls = CoreProperties.getLoader().getPropertyAsClass(
				TransactionMgr.class.getName() + ".SERIALIZABLE_CONCUR_MGR", SerializableConcurrencyMgr.class,
				ConcurrencyMgr.class);
		rrConcurMgrCls = CoreProperties.getLoader().getPropertyAsClass(
				TransactionMgr.class.getName() + ".REPEATABLE_READ_CONCUR_MGR", RepeatableReadConcurrencyMgr.class,
				ConcurrencyMgr.class);
		rcConcurMgrCls = CoreProperties.getLoader().getPropertyAsClass(
				TransactionMgr.class.getName() + ".READ_COMMITTED_CONCUR_MGR", ReadCommittedConcurrencyMgr.class,
				ConcurrencyMgr.class);
		recoveryMgrCls = CoreProperties.getLoader().getPropertyAsClass(TransactionMgr.class.getName() + ".RECOVERY_MGR",
				RecoveryMgr.class, RecoveryMgr.class);
	}

	// Optimization for preventing becoming bottleneck when creating a
	// transaction
	// XXX: There is a potential risk for overflowing here
	// private Long[] threadTxNums = new Long[TaskMgr.THREAD_POOL_SIZE * 10];
	// private ReentrantReadWriteLock activeTxsLock = new
	// ReentrantReadWriteLock();

	// Old method for maintaining active transaction list
	// When the above optimization ready, switch to that one
	private Set<Long> activeTxs = new HashSet<Long>();

	private long nextTxNum = 0;
	// Optimization: Use separate lock for nextTxNum
	private Object txNumLock = new Object();

	public TransactionMgr() {
		// for (int i = 0; i < threadTxNums.length; i++)
		// threadTxNums[i] = -1L;
	}

	// public synchronized ArrayList<Transaction> getActiveTransactions() {
	// return activeTxs;
	// }

	@Override
	public void onTxCommit(Transaction tx) {

		// activeTxsLock.readLock().lock();
		// try {
		// threadTxNums[(int) Thread.currentThread().getId()] = -1L;
		// } finally {
		// activeTxsLock.readLock().unlock();
		// }

		synchronized (this) {
			activeTxs.remove(tx.getTransactionNumber());
		}
	}

	@Override
	public void onTxRollback(Transaction tx) {

		// activeTxsLock.readLock().lock();
		// try {
		// threadTxNums[(int) Thread.currentThread().getId()] = -1L;
		// } finally {
		// activeTxsLock.readLock().unlock();
		// }

		synchronized (this) {
			activeTxs.remove(tx.getTransactionNumber());

		}
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	/**
	 * Creates non-quiescent checkpoint record.
	 * 
	 * @param checkpointTx
	 *            the transaction that performs checkpointing
	 */
	public void createCheckpoint(Transaction checkpointTx) {
		// stop access new tx request and find out active txs by using a write
		// lock on threadTxNums
		List<Long> txNums;
		// for (Transaction tx : activeTxs)
		// if (tx.getTransactionNumber() != checkpointTx
		// .getTransactionNumber())
		// txNums.add(tx.getTransactionNumber());

		// activeTxsLock.writeLock().lock();
		// try {
		// for (Long l : threadTxNums) {
		// if (l >= 0) {
		// txNums.add(l);
		// }
		// }
		//
		// // flush all buffers
		// // CT : Pick a tx and call its bufferMgr flushAll, or get
		// // bufferPoolMgr (init it at VanillaDb)
		// checkpointTx.bufferMgr().flushAll();
		// // wrtie a checkpoint record and flush to disk
		// LogSeqNum lsn = checkpointTx.recoveryMgr().checkpoint(txNums);
		// VanillaDb.logMgr().flush(lsn);
		// System.out.println("Chkpnt :" + txNums);
		// } finally {
		// activeTxsLock.writeLock().unlock();
		// }

		// Old method
		synchronized (this) {
			txNums = new LinkedList<Long>(activeTxs);
			checkpointTx.bufferMgr().flushAll();
			LogSeqNum lsn = checkpointTx.recoveryMgr().checkpoint(txNums);
			VanillaDb.logMgr().flush(lsn);
		}
	}

	public Transaction newTransaction(int isolationLevel, boolean readOnly) {
		// Dispatch new transaction number
		long txNum = -1;
		synchronized (txNumLock) {
			txNum = nextTxNum;
			nextTxNum++;
		}
		return newTransaction(isolationLevel, readOnly, txNum);
	}

	public Transaction newTransaction(int isolationLevel, boolean readOnly, long txNum) {
		// Update next transaction number
		synchronized (txNumLock) {
			if (txNum >= nextTxNum)
				nextTxNum = txNum + 1;
		}
		return createTransaction(isolationLevel, readOnly, txNum);
	}

	public long getNextTxNum() {
		synchronized (txNumLock) {
			return nextTxNum;
		}
	}
	 
	public int getActiveTxCount() { 
		synchronized (this) { 
			return activeTxs.size(); 
		} 
	}

	private Transaction createTransaction(int isolationLevel, boolean readOnly, long txNum) {
		if (logger.isLoggable(Level.FINE))
			logger.fine("new transaction: " + txNum);

		// Create a recovery manager
		RecoveryMgr recoveryMgr = null;
		try {
			Class<?> partypes[] = new Class[2];
			partypes[0] = Long.TYPE;
			partypes[1] = Boolean.TYPE;
			Constructor<?> ct = recoveryMgrCls.getConstructor(partypes);
			recoveryMgr = (RecoveryMgr) ct.newInstance(new Long(txNum), new Boolean(readOnly));
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Create a buffer manager
		BufferMgr bufferMgr = new BufferMgr(txNum);

		// Create a concurrency manager
		ConcurrencyMgr concurMgr = null;

		switch (isolationLevel) {
		case Connection.TRANSACTION_SERIALIZABLE:
			try {
				Class<?> partypes[] = new Class[1];
				partypes[0] = Long.TYPE;
				Constructor<?> ct = serialConcurMgrCls.getConstructor(partypes);
				concurMgr = (ConcurrencyMgr) ct.newInstance(new Long(txNum));
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case Connection.TRANSACTION_REPEATABLE_READ:
			try {
				Class<?> partypes[] = new Class[1];
				partypes[0] = Long.TYPE;
				Constructor<?> ct = rrConcurMgrCls.getConstructor(partypes);
				concurMgr = (ConcurrencyMgr) ct.newInstance(new Long(txNum));
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case Connection.TRANSACTION_READ_COMMITTED:
			try {
				Class<?> partypes[] = new Class[1];
				partypes[0] = Long.TYPE;
				Constructor<?> ct = rcConcurMgrCls.getConstructor(partypes);
				concurMgr = (ConcurrencyMgr) ct.newInstance(new Long(txNum));
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		default:
			throw new UnsupportedOperationException("unsupported isolation level");
		}

		Transaction tx = new Transaction(this, concurMgr, recoveryMgr, bufferMgr, readOnly, txNum);

		// activeTxsLock.readLock().lock();
		// try {
		// threadTxNums[(int) Thread.currentThread().getId()] = txNum;
		// } finally {
		// activeTxsLock.readLock().unlock();
		// }

		synchronized (this) {
			activeTxs.add(tx.getTransactionNumber());
		}
		return tx;
	}
}
