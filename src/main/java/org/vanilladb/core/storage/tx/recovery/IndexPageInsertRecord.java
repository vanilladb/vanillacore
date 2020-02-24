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
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.btree.BTreeDir;
import org.vanilladb.core.storage.index.btree.BTreeLeaf;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

public class IndexPageInsertRecord implements LogRecord {
	private long txNum;
	private BlockId indexBlkId;
	private int slotId;
	private boolean isDirPage;
	private SearchKeyType keyType;
	private LogSeqNum lsn;

	public IndexPageInsertRecord(long txNum, BlockId indexBlkId, boolean isDirPage,
			SearchKeyType keyType, int slotId) {
//		System.out.println(String.format("Tx.%d logs inserts on %s", txNum, indexBlkId));
		
		this.txNum = txNum;
		this.isDirPage = isDirPage;
		this.keyType = keyType;
		this.indexBlkId = indexBlkId;
		this.slotId = slotId;
	}

	public IndexPageInsertRecord(BasicLogRecord rec) {
		txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		isDirPage = (Integer) rec.nextVal(INTEGER).asJavaVal() == 1;
		
		// Search Key Type
		int keyLen = (Integer) rec.nextVal(INTEGER).asJavaVal();
		Type[] types = new Type[keyLen];
		for (int i = 0; i < keyLen; i++) {
			int type = (Integer) rec.nextVal(INTEGER).asJavaVal();
			int argument = (Integer) rec.nextVal(INTEGER).asJavaVal();
			types[i] = Type.newInstance(type, argument);
		}
		keyType = new SearchKeyType(types);
		
		String fileName = (String) rec.nextVal(VARCHAR).asJavaVal();
		long blkNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		indexBlkId = new BlockId(fileName, blkNum);
		slotId = (Integer) rec.nextVal(INTEGER).asJavaVal();
		lsn = rec.getLSN();
	}

	@Override
	public LogSeqNum writeToLog() {
		List<Constant> rec = buildRecord();
		return logMgr.append(rec.toArray(new Constant[rec.size()]));

	}

	@Override
	public int op() {
		return OP_INDEX_PAGE_INSERT;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	@Override
	public void undo(Transaction tx) {
//		System.out.println(String.format("Tx.%d undo inserting slot %d to %s", tx.getTransactionNumber(), slotId, indexBlkId));
		
		// Note that UndoNextLSN should be set to this log record's lsn in order
		// to let RecoveryMgr to skip this log record. Since this record should
		// be undo by the Clr append there.
		// Since Clr is Undo's redo log , here we should log
		// "IndexPageDeletionClr" to make this undo procedure be redo during
		// repeat history
		Buffer buff;
		if (isDirPage) {
			buff = tx.bufferMgr().pin(indexBlkId);
			if (this.lsn.compareTo(buff.lastLsn()) < 0) {
				BTreeDir.deleteASlot(tx, indexBlkId, keyType, slotId);
				LogSeqNum lsn = tx.recoveryMgr().logIndexPageDeletionClr(
						txNum, indexBlkId, isDirPage, keyType, slotId, this.lsn);
				VanillaDb.logMgr().flush(lsn);
			}

		} else {
			buff = tx.bufferMgr().pin(indexBlkId);
			if (this.lsn.compareTo(buff.lastLsn()) < 0) {
				BTreeLeaf.deleteASlot(tx, indexBlkId, keyType, slotId);
				LogSeqNum lsn = tx.recoveryMgr().logIndexPageDeletionClr(
						txNum, indexBlkId, isDirPage, keyType, slotId, this.lsn);
				VanillaDb.logMgr().flush(lsn);
			}
		}
		tx.bufferMgr().unpin(buff);

	}

	@Override
	public void redo(Transaction tx) {
		Buffer BlockBuff;
		if (isDirPage) {
			BlockBuff = tx.bufferMgr().pin(indexBlkId);

			if (this.lsn.compareTo(BlockBuff.lastLsn()) > 0) {
				BTreeDir.insertASlot(tx, indexBlkId, keyType, slotId);
			}
		} else {
			BlockBuff = tx.bufferMgr().pin(indexBlkId);
			if (this.lsn.compareTo(BlockBuff.lastLsn()) > 0) {
				BTreeLeaf.insertASlot(tx, indexBlkId, keyType, slotId);
			}
		}
		tx.bufferMgr().unpin(BlockBuff);

	}

	@Override
	public String toString() {
		return "<INDEX PAGE INSERT " + txNum + " " + isDirPage + " "
				+ keyType + " " + indexBlkId + " " + slotId + ">";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		// Covert Boolean into int
		rec.add(new IntegerConstant(isDirPage ? 1 : 0));
		
		// Search Key Type
		rec.add(new IntegerConstant(keyType.length()));
		for (int i = 0; i < keyType.length(); i++) {
			Type type = keyType.get(i);
			rec.add(new IntegerConstant(type.getSqlType()));
			rec.add(new IntegerConstant(type.getArgument()));
		}
		
		rec.add(new VarcharConstant(indexBlkId.fileName()));
		rec.add(new BigIntConstant(indexBlkId.number()));
		rec.add(new IntegerConstant(slotId));
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {

		return lsn;
	}

}
