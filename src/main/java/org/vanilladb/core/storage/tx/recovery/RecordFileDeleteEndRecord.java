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
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.util.LinkedList;
import java.util.List;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class RecordFileDeleteEndRecord extends LogicalEndRecord implements LogRecord {
	private long txNum, blkNum;
	private String tblName;
	private int slotId;
	private LogSeqNum lsn;

	public RecordFileDeleteEndRecord(long txNum, String tblName, long blkNum, int slotId, LogSeqNum logicalStartLSN) {
		this.txNum = txNum;
		this.tblName = tblName;
		this.blkNum = blkNum;
		this.slotId = slotId;
		super.logicalStartLSN = logicalStartLSN;
		this.lsn = null;
	}

	public RecordFileDeleteEndRecord(BasicLogRecord rec) {
		txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		tblName = (String) rec.nextVal(VARCHAR).asJavaVal();
		blkNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		slotId = (Integer) rec.nextVal(INTEGER).asJavaVal();
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
		return OP_RECORD_FILE_DELETE_END;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	@Override
	public void undo(Transaction tx) {

		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
		// TODO should decide whether logging or not , "dolog"-> UNDO have to
		// log
		RecordFile rf = new RecordFile(ti, tx, true);
		BlockId blk = new BlockId(tblName + ".tbl", blkNum);
		rf.insert(new RecordId(blk, slotId));
		// Append a Logical Abort log at the end of the LogRecords
		LogSeqNum lsn = tx.recoveryMgr().logLogicalAbort(this.txNum, this.logicalStartLSN);
		VanillaDb.logMgr().flush(lsn);

	}

	/**
	 * Logical Record should not be redo since it would not do the same physical
	 * operations as the time it terminated.
	 * 
	 * @see LogRecord#redo(Transaction)
	 */
	@Override
	public void redo(Transaction tx) {

		// do nothing

	}

	@Override
	public String toString() {
		return "<RECORD FILE DELETE END " + txNum + " " + tblName + " " + blkNum + " " + slotId + " " + lsn + ">";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		rec.add(new VarcharConstant(tblName));
		rec.add(new BigIntConstant(blkNum));
		rec.add(new IntegerConstant(slotId));
		rec.add(new BigIntConstant(super.logicalStartLSN.blkNum()));
		rec.add(new BigIntConstant(super.logicalStartLSN.offset()));
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {

		return lsn;
	}

}
