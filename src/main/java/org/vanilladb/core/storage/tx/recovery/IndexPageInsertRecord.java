/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
import org.vanilladb.core.storage.index.btree.BTreeDir;
import org.vanilladb.core.storage.index.btree.BTreeLeaf;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

public class IndexPageInsertRecord implements LogRecord {
	private long txNum, blkNum;
	private String indexName;
	private int slotId;
	private boolean isDirPage;
	private Type keyType;
	private LogSeqNum lsn;

	public IndexPageInsertRecord(long txNum, String indexName,
			Boolean isDirPage, Type keyType, long blkNum, int slotId) {
		this.txNum = txNum;
		this.indexName = indexName;
		this.isDirPage = isDirPage;
		this.keyType = keyType;
		this.blkNum = blkNum;
		this.slotId = slotId;
		this.lsn = null;

	}

	public IndexPageInsertRecord(BasicLogRecord rec) {
		txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		indexName = (String) rec.nextVal(VARCHAR).asJavaVal();
		isDirPage = (Integer) rec.nextVal(INTEGER).asJavaVal() == 1;
		keyType = Type.newInstance((Integer) rec.nextVal(INTEGER).asJavaVal());
		blkNum = (Long) rec.nextVal(BIGINT).asJavaVal();
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
		// Note that UndoNextLSN should be set to this log record's lsn in order
		// to let RecoveryMgr to skip this log record. Since this record should
		// be undo by the Clr append there.
		// Since Clr is Undo's redo log , here we should log
		// "IndexPageDeletionClr" to make this undo procedure be redo during
		// repeat history
		Buffer BlockBuff;
		if (isDirPage) {
			BlockId PageBlk = new BlockId(BTreeDir.getFileName(indexName),
					blkNum);
			BlockBuff = tx.bufferMgr().pin(PageBlk);
			if (this.lsn.compareTo(BlockBuff.lastLsn()) == 1) {
				BTreeDir.deleteASlot(tx, indexName, keyType, blkNum, slotId);
				LogSeqNum lsn = tx.recoveryMgr().logIndexPageDeletionClr(
						this.txNum, indexName, isDirPage, keyType, blkNum,
						slotId, this.lsn);
				VanillaDb.logMgr().flush(lsn);
			}

		} else {
			BlockId PageBlk = new BlockId(BTreeLeaf.getFileName(indexName),
					blkNum);
			BlockBuff = tx.bufferMgr().pin(PageBlk);
			if (this.lsn.compareTo(BlockBuff.lastLsn()) == 1) {
				BTreeLeaf.deleteASlot(tx, indexName, keyType, blkNum, slotId);
				LogSeqNum lsn = tx.recoveryMgr().logIndexPageDeletionClr(
						this.txNum, indexName, isDirPage, keyType, blkNum,
						slotId, this.lsn);
				VanillaDb.logMgr().flush(lsn);
			}
		}
		tx.bufferMgr().unpin(BlockBuff);

	}

	@Override
	public void redo(Transaction tx) {
		Buffer BlockBuff;
		if (isDirPage) {
			BlockId PageBlk = new BlockId(BTreeDir.getFileName(indexName),
					blkNum);
			BlockBuff = tx.bufferMgr().pin(PageBlk);

			if (this.lsn.compareTo(BlockBuff.lastLsn()) == 1) {
				BTreeDir.insertASlot(tx, indexName, keyType, blkNum, slotId);
			}
		} else {
			BlockId PageBlk = new BlockId(BTreeLeaf.getFileName(indexName),
					blkNum);
			BlockBuff = tx.bufferMgr().pin(PageBlk);
			if (this.lsn.compareTo(BlockBuff.lastLsn()) == 1) {
				BTreeLeaf.insertASlot(tx, indexName, keyType, blkNum, slotId);
			}
		}
		tx.bufferMgr().unpin(BlockBuff);

	}

	@Override
	public String toString() {
		return "<INDEX PAGE INSERT " + txNum + " " + indexName + " "
				+ isDirPage + " " + keyType.getSqlType() + " " + blkNum + " "
				+ slotId + ">";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		rec.add(new VarcharConstant(indexName));
		// Covert Boolean into int
		rec.add(new IntegerConstant(isDirPage ? 1 : 0));
		rec.add(new IntegerConstant(keyType.getSqlType()));
		rec.add(new BigIntConstant(blkNum));
		rec.add(new IntegerConstant(slotId));
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {

		return lsn;
	}

}
