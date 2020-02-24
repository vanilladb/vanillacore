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
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

class SetValueRecord implements LogRecord {
	private long txNum;
	private int offset;
	private Type type;
	private Constant val;
	private Constant newVal;
	private BlockId blk;
	private LogSeqNum lsn;

	/**
	 * Creates a new setval log record.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 * @param blk
	 *            the block containing the value
	 * @param offset
	 *            the offset of the value in the block
	 * @param val
	 *            the old value
	 * @param newVal
	 *            the new value
	 */
	public SetValueRecord(long txNum, BlockId blk, int offset, Constant val, Constant newVal) {
		this.txNum = txNum;
		this.blk = blk;
		this.offset = offset;
		this.type = val.getType();
		this.val = val;
		this.newVal = newVal;
		this.lsn = null;
	}

	/**
	 * Creates a log record by reading six other values from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 * @param op
	 *            the operation ID
	 */
	public SetValueRecord(BasicLogRecord rec) {
		txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		blk = new BlockId((String) rec.nextVal(VARCHAR).asJavaVal(), (Long) rec.nextVal(BIGINT).asJavaVal());
		offset = (Integer) rec.nextVal(INTEGER).asJavaVal();
		int sqlType = (Integer) rec.nextVal(INTEGER).asJavaVal();
		int sqlArg = (Integer) rec.nextVal(INTEGER).asJavaVal();
		type = Type.newInstance(sqlType, sqlArg);
		val = rec.nextVal(type);
		newVal = rec.nextVal(type);
		lsn = rec.getLSN();
	}

	/**
	 * Writes a setval record to the log. This log record contains the SQL type
	 * corresponding to the value as the operator ID, followed by the
	 * transaction ID, the filename, block number, and offset of the modified
	 * block, and the previous integer value at that offset.
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
		return OP_SET_VALUE;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	@Override
	public String toString() {
		return "<SETVAL " + op() + " " + txNum + " " + blk + " " + offset + " " + type + " " + val + " " + newVal + ">";
	}

	/**
	 * Replaces the specified data value with the value saved in the log record.
	 * The method pins a buffer to the specified block, calls setInt to restore
	 * the saved value (using a dummy LSN), and unpins the buffer.
	 * 
	 * @see LogRecord#undo(Transaction)
	 */
	@Override
	public void undo(Transaction tx) {
		Buffer buff = tx.bufferMgr().pin(blk);
		
		LogSeqNum lsn = tx.recoveryMgr().logSetValClr(this.txNum, buff, offset, val, this.lsn);
		VanillaDb.logMgr().flush(lsn);
		
		buff.setVal(offset, val, tx.getTransactionNumber(), null);
		tx.bufferMgr().unpin(buff);
		// Note that UndoNextLSN should be set to this log record's lsn in order
		// to let RecoveryMgr to skip this log record. Since this record should
		// be undo by the Clr append there.
		// Since Clr is Undo's redo log , here we should log
		// old val setVal log to make this undo procedure be redo during
		// repeat history

	}

	/**
	 * Replaces the specified data value with the new value saved in the log
	 * record. The method pins a buffer to the specified block, calls setInt to
	 * restore the saved value (using a dummy LSN), and unpins the buffer.
	 * 
	 * @see LogRecord#redo(Transaction)
	 */
	@Override
	public void redo(Transaction tx) {
		Buffer buff = tx.bufferMgr().pin(blk);
		buff.setVal(offset, newVal, tx.getTransactionNumber(), null);
		tx.bufferMgr().unpin(buff);
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		rec.add(new VarcharConstant(blk.fileName()));
		rec.add(new BigIntConstant(blk.number()));
		rec.add(new IntegerConstant(offset));
		rec.add(new IntegerConstant(type.getSqlType()));
		rec.add(new IntegerConstant(type.getArgument()));
		rec.add(val);
		rec.add(newVal);
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {
		return lsn;
	}

}
