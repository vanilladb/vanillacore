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
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The checkpoint log record.
 */
class CheckpointRecord implements LogRecord {
	private List<Long> txNums;
	private LogSeqNum lsn;
	/**
	 * Creates a quiescent checkpoint record.
	 */
	public CheckpointRecord() {
		this.txNums = new ArrayList<Long>();
	}

	/**
	 * Creates a non-quiescent checkpoint record.
	 */
	public CheckpointRecord(List<Long> txNums) {
		this.txNums = txNums;
		
	}

	/**
	 * Creates a log record by reading no other values from the basic log
	 * record.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public CheckpointRecord(BasicLogRecord rec) {
		int txCount = (Integer) rec.nextVal(INTEGER).asJavaVal();
		this.lsn = rec.getLSN();
		this.txNums = new ArrayList<Long>();
		for (int i = 0; i < txCount; i++) {
			txNums.add((Long) rec.nextVal(BIGINT).asJavaVal());
		}
	}

	/**
	 * Writes a checkpoint record to the log. This log record contains the
	 * {@link LogRecord#OP_CHECKPOINT} operator ID, number of active transctions
	 * during checkpointing and a list of active transaction ids.
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
		return OP_CHECKPOINT;
	}

	/**
	 * Checkpoint records have no associated transaction, and so the method
	 * returns a "dummy", negative txid.
	 */
	@Override
	public long txNumber() {
		return -1; // dummy value
	}

	/**
	 * Does nothing, because a checkpoint record contains no undo information.
	 */
	@Override
	public void undo(Transaction tx) {
		// do nothing
		
	}

	/**
	 * Does nothing, because a checkpoint record contains no redo information.
	 */
	@Override
	public void redo(Transaction tx) {
		// do nothing
	}

	@Override
	public String toString() {
		StringBuffer strbuf = new StringBuffer("<NQCKPT ");
		
		for (Long l : txNums) {
			strbuf.append(l + ",");
		}
		
		if (txNums.size() > 0)
			strbuf.delete(strbuf.length() - 1, strbuf.length());
		
		return strbuf.toString() + ">";
	}

	public List<Long> activeTxNums() {
		return this.txNums;
	}

	public boolean isContainTxNum(long txNum) {
		return this.txNums.contains(txNum);
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new IntegerConstant(txNums.size()));
		int recLength = txNums.size();
		for (int i = 0; i < recLength; i++)
			rec.add(new BigIntConstant(txNums.get(i)));
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {
		return lsn;
	}
}
