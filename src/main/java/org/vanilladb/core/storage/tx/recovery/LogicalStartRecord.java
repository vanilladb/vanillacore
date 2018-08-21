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

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

public class LogicalStartRecord implements LogRecord {
	private long txNum;
	private LogSeqNum lsn;

	public LogicalStartRecord(long txNum) {
		this.txNum = txNum;
		this.lsn = null;
	}

	public LogicalStartRecord(BasicLogRecord rec) {
		this.txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		lsn = rec.getLSN();
	}

	/**
	 * Returns the log sequence number of this log record.
	 * 
	 * @return the LSN
	 */
	public LogSeqNum getLSN() {
		return lsn;
	}

	@Override
	public LogSeqNum writeToLog() {
		List<Constant> rec = buildRecord();
		return logMgr.append(rec.toArray(new Constant[rec.size()]));
	}

	@Override
	public int op() {
		return OP_LOGICAL_START;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	/**
	 * Appends a Logical Abort Record to indicate the logical operation has be
	 * aborted
	 */
	@Override
	public void undo(Transaction tx) {

		LogSeqNum lsn = tx.recoveryMgr().logLogicalAbort(this.txNum,this.lsn);
		VanillaDb.logMgr().flush(lsn);

	}

	/**
	 * Does nothing, because a logical start record contains no redo
	 * information.
	 */
	@Override
	public void redo(Transaction tx) {
		// do nothing

	}

	@Override
	public String toString() {
		return "<LOGICAL START " + txNum + ">";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		return rec;
	}
}
