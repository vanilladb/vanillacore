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
package org.vanilladb.core.storage.tx.recovery;

import java.util.List;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.log.LogMgr;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The interface to be implemented by each type of log record.
 */
public interface LogRecord {
	/**
	 * @see LogRecord#op()
	 */
	static final int OP_CHECKPOINT = -41, OP_START = -42, OP_COMMIT = -43, OP_ROLLBACK = -44, OP_INDEX_INSERT = -45,
			OP_INDEX_DELETE = -46, OP_LOGICAL_START = -61, OP_SET_VALUE = -62, OP_LOGICAL_ABORT = -77,
			OP_RECORD_FILE_INSERT_END = -71, OP_RECORD_FILE_DELETE_END = -72, OP_INDEX_FILE_INSERT_END = -73,
			OP_INDEX_FILE_DELETE_END = -74, OP_INDEX_PAGE_INSERT = -75, OP_INDEX_PAGE_DELETE = -76,
			OP_SET_VALUE_CLR = -78, OP_INDEX_PAGE_INSERT_CLR = -79, OP_INDEX_PAGE_DELETE_CLR = -80;

	static LogMgr logMgr = VanillaDb.logMgr();

	/**
	 * Build the constants for the physical log record and return them as a
	 * list.
	 * 
	 * @return a list containing the constants for a log record
	 */
	List<Constant> buildRecord();

	/**
	 * Writes the record to the log and returns its LSN.
	 * 
	 * @return the LSN of the record in the log
	 */
	LogSeqNum writeToLog();

	/**
	 * Returns IDs used to distinguish different logged operations. Depending on
	 * the type of value being set, the operation ID of the
	 * {@link SetValueRecord} equals to the corresponding SQL type. Thus all
	 * other operations cannot have IDs equal to the values defined in
	 * {@link java.sql.Types}.
	 * 
	 * @return the operation ID
	 */
	int op();

	/**
	 * Returns the transaction id stored with the log record.
	 * 
	 * @return the log record's transaction id
	 */
	long txNumber();
	
	/**
	 * Returns the log sequence number of this log record.
	 * 
	 * @return the LSN
	 */
	LogSeqNum getLSN();

	/**
	 * Undoes the operation encoded by this log record.
	 * 
	 * @param tx
	 *            the transaction that is performing the undo.
	 */
	void undo(Transaction tx);

	/**
	 * Redoes the operation encoded by this log record.
	 * 
	 * @param tx
	 *            the transaction that is performing the redo.
	 */
	void redo(Transaction tx);
}
