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

import static org.vanilladb.core.sql.Type.BIGINT;

import java.util.LinkedList;
import java.util.List;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

class StartRecord implements LogRecord {
	private long txNum;
	private LogSeqNum lsn;

	/**
	 * Creates a new start log record for the specified transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 */
	public StartRecord(long txNum) {
		this.txNum = txNum;
		this.lsn = null;
	}

	/**
	 * Creates a log record by reading one other value from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public StartRecord(BasicLogRecord rec) {
		txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		lsn = rec.getLSN();
	}

	/**
	 * Writes a start record to the log. This log record contains the
	 * {@link LogRecord#OP_START} operator ID, followed by the transaction ID.
	 * 
	 * @return the LSN of the log record
	 */
	@Override
	public LogSeqNum writeToLog() {
		List<Constant> rec = buildRecord();
		return logMgr.append(rec.toArray(new Constant[rec.size()]));
	}

	@Override
	public int op() {
		return OP_START;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	/**
	 * Does nothing, because a start record contains no undo information.
	 */
	@Override
	public void undo(Transaction tx) {
		// do nothing
	}

	/**
	 * Does nothing, because a start record contains no redo information.
	 */
	@Override
	public void redo(Transaction tx) {
		// do nothing
	}

	@Override
	public String toString() {
		return "<START " + txNum + ">";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {
		return lsn;
	}
}
