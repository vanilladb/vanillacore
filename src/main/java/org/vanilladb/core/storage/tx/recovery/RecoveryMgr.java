/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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
package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_CHECKPOINT;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_COMMIT;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_ROLLBACK;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_START;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionLifecycleListener;

/**
 * The recovery manager. Each transaction has its own recovery manager.
 */
public class RecoveryMgr implements TransactionLifecycleListener {

	private static boolean enableLogging = true;

	public static void enableLogging(boolean log) {
		enableLogging = log;
	}

	/**
	 * Goes through the log, rolling back all uncompleted transactions. Flushes
	 * all modified blocks. Finally, writes a quiescent checkpoint record to the
	 * log and flush it. This method should be called only during system
	 * startup, before user transactions begin.
	 * 
	 * @param tx
	 *            the context of executing transaction
	 */
	public static void initializeSystem(Transaction tx) {
		tx.recoveryMgr().recoverSystem(tx);
		tx.bufferMgr().flushAll();
		VanillaDb.logMgr().removeAndCreateNewLog();
		
		// Add a start record for this transaction
		new StartRecord(tx.getTransactionNumber()).writeToLog();
	}

	private Map<Long, LogSeqNum> txUnDoNextLSN = new HashMap<Long, LogSeqNum>();
	private long txNum; // the owner id of this recovery manger
	private LogSeqNum logicalStartLSN = null;

	/**
	 * Creates a recovery manager for the specified transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 * @param isReadOnly
	 *            is the transaction read-only
	 */
	public RecoveryMgr(long txNum, boolean isReadOnly) {
		this.txNum = txNum;
		if (!isReadOnly && enableLogging)
			new StartRecord(txNum).writeToLog();
	}

	/**
	 * Writes a commit record to the log, and then flushes the log record to
	 * disk.
	 * 
	 * @param tx
	 *            the context of committing transaction
	 */
	@Override
	public void onTxCommit(Transaction tx) {
		if (!tx.isReadOnly() && enableLogging) {
			LogSeqNum lsn = new CommitRecord(txNum).writeToLog();
			VanillaDb.logMgr().flush(lsn);
		}
	}

	/**
	 * Does the roll back process, writes a rollback record to the log, and
	 * flushes the log record to disk.
	 */
	@Override
	public void onTxRollback(Transaction tx) {
		if (!tx.isReadOnly() && enableLogging) {
			rollback(tx);
			LogSeqNum lsn = new RollbackRecord(txNum).writeToLog();
			VanillaDb.logMgr().flush(lsn);
		}
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	/**
	 * Writes a checkpoint record to the log.
	 * 
	 * @param txNums
	 *            the transactions that are being executed when writing the
	 *            checkpoint.
	 * @return the LSN of the log record.
	 */
	public LogSeqNum checkpoint(List<Long> txNums) {
		return new CheckpointRecord(txNums).writeToLog();
	}

	/**
	 * Writes a set value record to the log.
	 * 
	 * @param buff
	 *            the buffer containing the page
	 * @param offset
	 *            the offset of the value in the page
	 * @param newVal
	 *            the value to be written
	 * @return the LSN of the log record, or -1 if updates to temporary files
	 */
	public LogSeqNum logSetVal(Buffer buff, int offset, Constant newVal) {
		if (enableLogging) {
			BlockId blk = buff.block();
			if (isTempBlock(blk))
				return null;
			return new SetValueRecord(txNum, blk, offset, buff.getVal(offset, newVal.getType()), newVal).writeToLog();
		} else
			return null;
	}

	public LogSeqNum logLogicalStart() {
		if (enableLogging) {
			// Store logicalStartLSN inside the RecoveryMgr
			this.logicalStartLSN = new LogicalStartRecord(txNum).writeToLog();
			return this.logicalStartLSN;
		} else
			return null;
	}

	/**
	 * Writes a logical abort record into the log.
	 * 
	 * @param txNum
	 *            the number of aborted transaction
	 * @param undoNextLSN
	 *            the LSN which the redo the Abort record should jump to
	 * 
	 * @return the LSN of the log record, or null if recovery manager turns off
	 *         the logging
	 */
	public LogSeqNum logLogicalAbort(long txNum, LogSeqNum undoNextLSN) {
		if (enableLogging) {
			return new LogicalAbortRecord(txNum, undoNextLSN).writeToLog();
		} else
			return null;
	}

	public LogSeqNum logRecordFileInsertionEnd(String tblName, long blkNum, int slotId) {
		if (enableLogging) {
			if (this.logicalStartLSN == null)
				throw new RuntimeException("Logical start LSN is null (in logRecordFileInsertionEnd)");
			LogSeqNum lsn = new RecordFileInsertEndRecord(txNum, tblName, blkNum, slotId, this.logicalStartLSN)
					.writeToLog();
			this.logicalStartLSN = null;
			return lsn;
		} else
			return null;
	}

	public LogSeqNum logRecordFileDeletionEnd(String tblName, long blkNum, int slotId) {
		if (enableLogging) {
			if (this.logicalStartLSN == null)
				throw new RuntimeException("Logical start LSN is null (in logRecordFileDeletionEnd)");
			LogSeqNum lsn = new RecordFileDeleteEndRecord(txNum, tblName, blkNum, slotId, this.logicalStartLSN)
					.writeToLog();
			this.logicalStartLSN = null;
			return lsn;
		} else
			return null;
	}

	public LogSeqNum logIndexInsertionEnd(String indexName, SearchKey searchKey, long recordBlockNum,
			int recordSlotId) {
		if (enableLogging) {
			if (this.logicalStartLSN == null)
				throw new RuntimeException("Logical start LSN is null (in logIndexInsertionEnd)");
			LogSeqNum lsn = new IndexInsertEndRecord(txNum, indexName, searchKey, recordBlockNum, recordSlotId,
					this.logicalStartLSN).writeToLog();
			this.logicalStartLSN = null;
			return lsn;
		} else
			return null;
	}

	public LogSeqNum logIndexDeletionEnd(String indexName, SearchKey searchKey, long recordBlockNum, int recordSlotId) {
		if (enableLogging) {
			if (this.logicalStartLSN == null)
				throw new RuntimeException("Logical start LSN is null (in logIndexDeletionEnd)");
			LogSeqNum lsn = new IndexDeleteEndRecord(txNum, indexName, searchKey, recordBlockNum, recordSlotId,
					this.logicalStartLSN).writeToLog();
			this.logicalStartLSN = null;
			return lsn;
		} else
			return null;
	}

	public LogSeqNum logIndexPageInsertion(BlockId indexBlkId, boolean isDirPage, SearchKeyType keyType, int slotId) {
		if (enableLogging) {
			return new IndexPageInsertRecord(txNum, indexBlkId, isDirPage, keyType, slotId).writeToLog();
		} else
			return null;
	}

	public LogSeqNum logIndexPageDeletion(BlockId indexBlkId, boolean isDirPage, SearchKeyType keyType, int slotId) {
		if (enableLogging) {
			return new IndexPageDeleteRecord(txNum, indexBlkId, isDirPage, keyType, slotId).writeToLog();
		} else
			return null;
	}

	public LogSeqNum logIndexPageInsertionClr(long compTxNum, BlockId indexBlkId, boolean isDirPage,
			SearchKeyType keyType, int slotId, LogSeqNum undoNextLSN) {
		if (enableLogging) {
			return new IndexPageInsertClr(compTxNum, indexBlkId, isDirPage, keyType, slotId, undoNextLSN).writeToLog();
		} else
			return null;
	}

	public LogSeqNum logIndexPageDeletionClr(long compTxNum, BlockId indexBlkId, boolean isDirPage,
			SearchKeyType keyType, int slotId, LogSeqNum undoNextLSN) {
		if (enableLogging) {
			return new IndexPageDeleteClr(compTxNum, indexBlkId, isDirPage, keyType, slotId, undoNextLSN).writeToLog();
		} else
			return null;
	}

	public LogSeqNum logSetValClr(long compTxNum, Buffer buff, int offset, Constant newVal, LogSeqNum undoNextLSN) {
		if (enableLogging) {
			BlockId blk = buff.block();
			if (isTempBlock(blk))
				return null;
			return new SetValueClr(compTxNum, blk, offset, buff.getVal(offset, newVal.getType()), newVal, undoNextLSN)
					.writeToLog();
		} else
			return null;
	}

	/**
	 * Rolls back the transaction. The method iterates through the log records,
	 * calling {@link LogRecord#undo(Transaction)} for each log record it finds
	 * for the transaction, until it finds the transaction's START record.
	 */
	void rollback(Transaction tx) {
		ReversibleIterator<LogRecord> iter = new LogRecordIterator();
		LogSeqNum txUnDoNextLSN = null;
		while (iter.hasNext()) {
			LogRecord rec = iter.next();
			if (rec.txNumber() == txNum) {
				if (txUnDoNextLSN != null) {
					if (txUnDoNextLSN.compareTo(rec.getLSN()) != 1)
						continue;
				}
				if (rec.op() == OP_START)
					return;
				else if (rec instanceof LogicalEndRecord) {

					// Undo this Logical operation;
					rec.undo(tx);
					/*
					 * Extract the logicalStartLSN form rec by casting it as a
					 * LogicalEndRecord
					 */
					LogSeqNum logicalStartLSN = ((LogicalEndRecord) rec).getlogicalStartLSN();

					/*
					 * Save the Logical Start LSN to skip the log records
					 * between the end record and the start record
					 */
					txUnDoNextLSN = logicalStartLSN;

				} else
					rec.undo(tx);
			}
		}
	}

	void rollbackPartially(Transaction tx, int stepsInUndo) {
		ReversibleIterator<LogRecord> iter = new LogRecordIterator();
		LogSeqNum txUnDoNextLSN = null;
		while (iter.hasNext() && stepsInUndo >= 0) {
			LogRecord rec = iter.next();
			stepsInUndo--;
			if (rec.txNumber() == txNum) {

				if (txUnDoNextLSN != null) {
					if (txUnDoNextLSN.compareTo(rec.getLSN()) != 1)
						continue;
				}
				if (rec.op() == OP_START)
					return;
				else if (rec instanceof LogicalEndRecord) {

					rec.undo(tx);

					LogSeqNum logicalStartLSN = ((LogicalEndRecord) rec).getlogicalStartLSN();

					txUnDoNextLSN = logicalStartLSN;

				} else
					rec.undo(tx);
			}
		}
	}

	/**
	 * Does a complete database recovery. The method iterates through the log
	 * records. Whenever it finds a log record for an unfinished transaction, it
	 * calls {@link LogRecord#undo(Transaction)} on that record. The method
	 * stops iterating forward when it encounters a CHECKPOINT record and finds
	 * all the transactions which were executing when the checkpoint took place,
	 * or when the end of the log is reached. The method then iterates backward
	 * and redoes all finished transactions. TODO fix comments...
	 */
	void recoverSystem(Transaction tx) {
		Set<Long> finishedTxs = new HashSet<Long>();
		Set<Long> unCompletedTxs = new HashSet<Long>();

		List<Long> txsOnCheckpointing = null;
		ReversibleIterator<LogRecord> iter = new LogRecordIterator();
		/*
		 * analyze phase: Find the earliest unfinished txNum
		 */
		// analyze phase
		while (iter.hasNext()) {
			LogRecord rec = iter.next();

			int op = rec.op();
			if (op == OP_CHECKPOINT) {
				// Since we flush all dirtyPage at checkpoint, therefore no need
				// to find the start record of active txNum
				txsOnCheckpointing = ((CheckpointRecord) rec).activeTxNums();
				for (long acTxn : txsOnCheckpointing) {
					// txNum give us info of possible unFinshedTxs,
					// Check if those weren't in finishedTxs, and add it to the
					// uncompletedTxs
					if (!finishedTxs.contains(acTxn))
						unCompletedTxs.add(acTxn);
				}
				// Start Redo From checkpoint
				break;
			}

			if (op == OP_COMMIT) {
				finishedTxs.add(rec.txNumber());
			} else if (op == OP_ROLLBACK) {
				finishedTxs.add(rec.txNumber());
			} else if (op == OP_START && !finishedTxs.contains(rec.txNumber())) {
				unCompletedTxs.add(rec.txNumber());
			}

		}

		finishedTxs = null;
		/*
		 * redo phase: Repeating History
		 */

		while (iter.hasPrevious()) {
			LogRecord rec = iter.previous();

			rec.redo(tx);

		}

		// remove the recovery tx from unCompletedTxs set
		unCompletedTxs.remove(tx.getTransactionNumber());

		iter = new LogRecordIterator();
		/*
		 * undo phase: undo all actions performed by the active txs during last
		 * crash
		 */

		while (iter.hasNext()) {
			LogRecord rec = iter.next();

			int op = rec.op();
			if (!unCompletedTxs.contains(rec.txNumber()) || op == OP_COMMIT || op == OP_ROLLBACK)
				continue;
			/*
			 * Use UnDoNextLSN to skip unnecessary physical record which have
			 * been redo its undo by CLR or records have been rolled back
			 */

			if (txUnDoNextLSN.containsKey(rec.txNumber())) {
				if (txUnDoNextLSN.get(rec.txNumber()).compareTo(rec.getLSN()) != 1)
					continue;
			}
			if (op == OP_START)
				unCompletedTxs.remove(rec.txNumber());
			else if (rec instanceof LogicalEndRecord) {

				// Undo this Logical operation;
				rec.undo(tx);
				/*
				 * Extract the logicalStartLSN form rec by casting it as a
				 * LogicalEndRecord
				 */
				LogSeqNum logicalStartLSN = ((LogicalEndRecord) rec).getlogicalStartLSN();

				/*
				 * Save the Logical Start LSN to skip the log records between
				 * the end record and the start record
				 */
				txUnDoNextLSN.put(rec.txNumber(), logicalStartLSN);

			} else if (rec instanceof CompesationLogRecord) {
				/*
				 * Extract the logicalStartLSN form rec by casting it as a
				 * LogicalEndRecord
				 */
				LogSeqNum undoNextLSN = ((CompesationLogRecord) rec).getUndoNextLSN();
				/*
				 * Save the UndoNext LSN to skip the records have been rolled
				 * back
				 */
				txUnDoNextLSN.put(rec.txNumber(), undoNextLSN);
			} else
				rec.undo(tx);

			if (unCompletedTxs.size() == 0)
				break;

		}
	}

	void recoverSystemPartially(Transaction tx, int stepsInUndo) {
		Set<Long> finishedTxs = new HashSet<Long>();
		Set<Long> unCompletedTxs = new HashSet<Long>();

		List<Long> txsOnCheckpointing = null;
		ReversibleIterator<LogRecord> iter = new LogRecordIterator();
		/*
		 * analyze phase: Find the earliest unfinished txNum
		 */
		while (iter.hasNext()) {
			LogRecord rec = iter.next();

			int op = rec.op();
			if (op == OP_CHECKPOINT) {
				txsOnCheckpointing = ((CheckpointRecord) rec).activeTxNums();
				for (long acTxn : txsOnCheckpointing) {

					if (!finishedTxs.contains(acTxn))
						unCompletedTxs.add(acTxn);
				}
				break;
			}

			if (op == OP_COMMIT) {
				finishedTxs.add(rec.txNumber());
			} else if (op == OP_ROLLBACK) {
				finishedTxs.add(rec.txNumber());
			} else if (op == OP_START && !finishedTxs.contains(rec.txNumber())) {
				unCompletedTxs.add(rec.txNumber());
			}

		}

		finishedTxs = null;
		/*
		 * redo phase: Repeating History
		 */

		while (iter.hasPrevious()) {
			LogRecord rec = iter.previous();

			rec.redo(tx);

		}

		unCompletedTxs.remove(tx.getTransactionNumber());

		iter = new LogRecordIterator();
		/*
		 * undo phase: undo all actions performed by the active txs during last
		 * crash
		 */

		while (iter.hasNext() && stepsInUndo >= 0) {
			LogRecord rec = iter.next();
			// System.out.println(rec.getLSN() + rec.toString());
			stepsInUndo--;
			int op = rec.op();
			if (!unCompletedTxs.contains(rec.txNumber()) || op == OP_COMMIT || op == OP_ROLLBACK)
				continue;

			if (txUnDoNextLSN.containsKey(rec.txNumber())) {
				if (txUnDoNextLSN.get(rec.txNumber()).compareTo(rec.getLSN()) != 1)
					continue;
			}
			if (op == OP_START)
				unCompletedTxs.remove(rec.txNumber());
			else if (rec instanceof LogicalEndRecord) {

				rec.undo(tx);

				LogSeqNum logicalStartLSN = ((LogicalEndRecord) rec).getlogicalStartLSN();

				txUnDoNextLSN.put(rec.txNumber(), logicalStartLSN);

			} else if (rec instanceof CompesationLogRecord) {

				LogSeqNum undoNextLSN = ((CompesationLogRecord) rec).getUndoNextLSN();

				txUnDoNextLSN.put(rec.txNumber(), undoNextLSN);
			} else
				rec.undo(tx);

			if (unCompletedTxs.size() == 0)
				break;

		}
	}

	/**
	 * Determines whether a block comes from a temporary file or not.
	 */
	private boolean isTempBlock(BlockId blk) {
		return blk.fileName().startsWith("_temp");
	}

}
