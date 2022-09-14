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
package org.vanilladb.core.storage.record;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * Manages the placement and access of metadata in the file header.
 */
public class FileHeaderPage {
	/**
	 * The offset of pointers of the last deleted slot.
	 */
	protected static final int OFFSET_LDS_BLOCKID = 0, OFFSET_LDS_RID = Page
			.maxSize(BIGINT);
	
	/**
	 * The offset of pointers of the tail slot.
	 */
	protected static final int OFFSET_TS_BLOCKID = Page.maxSize(BIGINT)
			+ Page.maxSize(INTEGER), OFFSET_TS_RID = 2 * Page.maxSize(BIGINT)
			+ Page.maxSize(INTEGER);
	
	protected static final long NO_SLOT_BLOCKID = -1;
	protected static final int NO_SLOT_RID = -1;
	private Transaction tx;
	private BlockId blk;
	private Buffer currentBuff;
	private String fileName;

	/**
	 * Creates the header manager for a specified file.
	 * 
	 * @param fileName
	 *            the file name
	 * @param tx
	 *            the transaction
	 */
	public FileHeaderPage(String fileName, Transaction tx) {
		this.fileName = fileName;
		this.tx = tx;
		blk = new BlockId(fileName, 0);
		currentBuff = tx.bufferMgr().pin(blk);
	}

	/**
	 * Closes the header manager, by unpinning the block.
	 */
	public void close() {
		if (blk != null) {
			tx.bufferMgr().unpin(currentBuff);
			blk = null;
			currentBuff = null;
		}
	}

	/**
	 * Return true if this file has inserted data records.
	 * 
	 * @return true if this file has inserted data records
	 */
	public boolean hasDataRecords() {
		long blkNum = (Long) getVal(OFFSET_TS_BLOCKID, BIGINT).asJavaVal();
		return blkNum != NO_SLOT_BLOCKID ? true : false;
	}

	/**
	 * Return true if this file has deleted data records.
	 * 
	 * @return true if this file has deleted data records
	 */
	public boolean hasDeletedSlots() {
		long blkNum = (Long) getVal(OFFSET_LDS_BLOCKID, BIGINT).asJavaVal();
		return blkNum != NO_SLOT_BLOCKID ? true : false;
	}

	/**
	 * Returns the id of last deleted record.
	 * 
	 * @return the id of last deleted record
	 */
	public RecordId getLastDeletedSlot() {
		Constant blkNum = getVal(OFFSET_LDS_BLOCKID, BIGINT);
		Constant rid = getVal(OFFSET_LDS_RID, INTEGER);
		BlockId bid = new BlockId(fileName, (Long) blkNum.asJavaVal());
		return new RecordId(bid, (Integer) rid.asJavaVal());
	}

	/**
	 * Returns the id of tail slot.
	 * 
	 * @return the id of tail slot
	 */
	public RecordId getTailSolt() {
		Constant blkNum = getVal(OFFSET_TS_BLOCKID, BIGINT);
		Constant rid = getVal(OFFSET_TS_RID, INTEGER);
		BlockId bid = new BlockId(fileName, (Long) blkNum.asJavaVal());
		return new RecordId(bid, (Integer) rid.asJavaVal());
	}

	/**
	 * Set the id of last deleted record.
	 * 
	 * @param rid
	 *            the id of last deleted record
	 */
	public void setLastDeletedSlot(RecordId rid) {
		setVal(OFFSET_LDS_BLOCKID, new BigIntConstant(rid.block().number()));
		setVal(OFFSET_LDS_RID, new IntegerConstant(rid.id()));
	}

	/**
	 * Set the id of last tail slot.
	 * 
	 * @param rid
	 *            the id of tail slot
	 */
	public void setTailSlot(RecordId rid) {
		setVal(OFFSET_TS_BLOCKID, new BigIntConstant(rid.block().number()));
		setVal(OFFSET_TS_RID, new IntegerConstant(rid.id()));
	}

	private Constant getVal(int offset, Type type) {
		if (!isTempTable())
			tx.concurrencyMgr().readBlock(blk);
		return currentBuff.getVal(offset, type);
	}

	private void setVal(int offset, Constant val) {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();
		if (!isTempTable())
			tx.concurrencyMgr().modifyBlock(blk);
		LogSeqNum lsn = tx.recoveryMgr().logSetVal(currentBuff, offset, val);
		currentBuff.setVal(offset, val, tx.getTransactionNumber(), lsn);
	}

	private boolean isTempTable() {
		return blk.fileName().startsWith("_temp");
	}
}
