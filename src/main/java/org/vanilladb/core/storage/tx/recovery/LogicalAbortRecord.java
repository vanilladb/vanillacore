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

public class LogicalAbortRecord extends LogicalEndRecord implements LogRecord {
	private long txNum;
	private LogSeqNum lsn;

	public LogicalAbortRecord(long txNum, LogSeqNum logicalStartLSN) {
		this.txNum = txNum;
		super.logicalStartLSN = logicalStartLSN;
		this.lsn = null;
	}

	public LogicalAbortRecord(BasicLogRecord rec) {
		txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		super.logicalStartLSN = new LogSeqNum((Long) rec.nextVal(BIGINT).asJavaVal(),
				(Long) rec.nextVal(BIGINT).asJavaVal());
		lsn = rec.getLSN();
	}

	@Override
	public LogSeqNum writeToLog() {
		List<Constant> rec = buildRecord();
		return logMgr.append(rec.toArray(new Constant[rec.size()]));
	}

	@Override
	public int op() {
		return OP_LOGICAL_ABORT;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	/**
	 * Does nothing, because a Logical Abort record contains no undo
	 * information.
	 */
	@Override
	public void undo(Transaction tx) {
		// do nothing

	}

	/**
	 * Does nothing, because a Logical Abort record contains no undo
	 * information.
	 */
	@Override
	public void redo(Transaction tx) {
		// do nothing

	}

	@Override
	public String toString() {
		return "<LOGICAL ABORT " + txNum + " " + logicalStartLSN + ">";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		rec.add(new BigIntConstant(super.logicalStartLSN.blkNum()));
		rec.add(new BigIntConstant(super.logicalStartLSN.offset()));
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {

		return lsn;
	}

}
