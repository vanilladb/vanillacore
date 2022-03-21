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
package org.vanilladb.core.storage.index.btree;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.nio.BufferOverflowException;
import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * A page corresponding to a single B-tree block in a file for {@link BTreeDir}
 * or {@link BTreeLeaf}.
 * <p>
 * The content of each B-tree block begins with an integer storing the number of
 * index records in that page, then a series of integer flags, followed by a
 * series of slots holding index records. Index records are sorted in ascending
 * order.
 * </p>
 */
public class BTreePage {
	private BlockId blk;
	private Schema schema;
	private Transaction tx;
	private int slotSize, headerSize, numberOfSlots, numberOfFlags;
	private Buffer currentBuff;
	private Map<String, Integer> myOffsetMap;
	// Optimization: Materialize the number of records of B-Tree Page.
	private int numberOfRecords;

	// Optimization: Materialize the offset map.
	/**
	 * Returns the offset of a specified field within a record.
	 * 
	 * @param sch
	 *            the schema of the target index
	 * @param fldName
	 *            the name of the field
	 * 
	 * @return the offset of that field within a record
	 */
	@Deprecated
	public static int offset(Schema sch, String fldName) {
		int pos = 0;
		for (String fldname : sch.fields()) {
			if (fldName.equals(fldname))
				break;
			pos += Page.maxSize(sch.type(fldname));
		}
		return pos;
	}

	/**
	 * Returns the map of field name to offset of a specified schema.
	 * 
	 * @param sch
	 *            the table's schema
	 * 
	 * @return the offset map
	 */
	public static Map<String, Integer> offsetMap(Schema sch) {
		int pos = 0;
		Map<String, Integer> offsetMap = new HashMap<String, Integer>();

		for (String fldname : sch.fields()) {
			offsetMap.put(fldname, pos);
			pos += Page.maxSize(sch.type(fldname));
		}
		return offsetMap;
	}

	/**
	 * Returns the number of bytes required to store a record in disk.
	 * 
	 * @param sch
	 *            the schema of the target index
	 * @return the size of a record, in bytes
	 */
	public static int slotSize(Schema sch) {
		int size = 0;
		for (String fldname : sch.fields())
			size += Page.maxSize(sch.type(fldname));
		
		if (size > Buffer.BUFFER_SIZE || size < 0)
			throw new RuntimeException("Slot size overflow: " + size + ", schema: " + sch);
		
		return size;
	}

	public static int numOfSlots(int numOfFlags, Schema sch) {
		int slotSize = slotSize(sch);
		int flagSize = numOfFlags * Type.BIGINT.maxSize();
		return (Buffer.BUFFER_SIZE - flagSize) / slotSize;
	}

	/**
	 * Opens a page for the specified B-tree block.
	 * 
	 * @param blk
	 *            a block ID refers to the B-tree block
	 * @param numFlags
	 *            the number of flags in this b-tree page
	 * @param schema
	 *            the schema for the particular B-tree file
	 * @param tx
	 *            the calling transaction
	 */
	public BTreePage(BlockId blk, int numFlags, Schema schema, Transaction tx) {
		this.blk = blk;
		this.tx = tx;
		this.schema = schema;
		currentBuff = tx.bufferMgr().pin(blk);

		slotSize = slotSize(schema);
		// Slot: a place to hold a record. The number of slots are fixed.
		// Record: a record that contains meaningful information for index.
		// Note that a slot may not have a record. It could be empty.
		numberOfRecords = -1; // Cache number of records. Lazily evaluated.
		numberOfFlags = numFlags;
		numberOfSlots = numOfSlots(numFlags, schema);
		headerSize = Page.maxSize(INTEGER) + Page.maxSize(BIGINT) * numFlags;
		myOffsetMap = offsetMap(schema);
	}

	/**
	 * Closes the page by unpinning its buffer.
	 */
	public void close() {
		if (blk != null) {
			tx.bufferMgr().unpin(currentBuff);
			blk = null;
			currentBuff = null;
			numberOfRecords = -1;
		}
	}

	/**
	 * Returns the i-th flag.
	 * 
	 * @param i
	 *            flag index, starting from 0
	 * @return the i-th flag
	 */
	public long getFlag(int i) {
		return (Long) getVal(Page.maxSize(INTEGER) + Page.maxSize(BIGINT) * i, BIGINT).asJavaVal();
	}

	/**
	 * Sets the i-th flag.
	 * 
	 * @param i
	 *            flag index, starting from 0
	 * @param val
	 *            the flag value
	 */
	public void setFlag(int i, long val) {
		int offset = Page.maxSize(INTEGER) + Page.maxSize(BIGINT) * i;
		Constant v = new BigIntConstant(val);
		setVal(offset, v);
	}

	public Constant getVal(int slot, String fldName) {
		if (slot >= getNumRecords())
			throw new IndexOutOfBoundsException(
					String.format("Cannot get value at slot %d "
							+ "from BTreePage %s (which has only %d slot)",
							slot, blk, getNumRecords()));
		Type type = schema.type(fldName);
		return getVal(fieldPosition(slot, fldName), type);
	}

	/**
	 * Set the value of the specified field at the specified slot.
	 * 
	 * @param slot
	 *            the target slot
	 * @param fldName
	 *            the target field
	 * @param val
	 *            the new value
	 */
	public void setVal(int slot, String fldName, Constant val) {
		if (slot >= numberOfSlots) {
			throw new IndexOutOfBoundsException(
					String.format("Cannot set value at slot %d "
							+ "in BTreePage %s (which can only have %d slot)",
							slot, blk, numberOfSlots));
		} else if (slot >= numberOfRecords) {
			throw new IndexOutOfBoundsException(
				String.format("Cannot set value at slot %d "
						+ "in BTreePage %s because there are only %d records",
						slot, blk, numberOfRecords));
		}
		setValUnchecked(slot, fldName, val);
	}

	/**
	 * Set the value of the specified field at the specified slot. This method
	 * is designed for physiological operations. Since a physiological operation
	 * is only logged before and after the operation, no other log should be
	 * appended during the operation.
	 * 
	 * @param slot
	 *            the target slot
	 * @param fldName
	 *            the target field
	 * @param val
	 *            the new value
	 */
	private void setValWithoutLogging(int slot, String fldName, Constant val) {
		Type type = schema.type(fldName);
		Constant v = val.castTo(type);
		setValWithoutLogging(fieldPosition(slot, fldName), v);
	}

	/**
	 * Inserts a slot to the current BTreePage. Since this whole action must be
	 * done atomically in a buffer, it will lock the flushing mechanism of the
	 * buffer to ensure no one can flush during the operation.
	 * 
	 * @param slot
	 *            the id of the slot to be inserted
	 */
	public void insert(int slot) {
		currentBuff.lockFlushing();
		try {
			if (slot >= numberOfSlots) {
				throw new IndexOutOfBoundsException(
						String.format("Cannot insert a record at slot %d "
								+ "in BTreePage %s because there are only %d slots",
								slot, blk, numberOfSlots));
			} else if (numberOfRecords + 1 > numberOfSlots) {
				throw new BufferOverflowException();
			}
			
			for (int i = getNumRecords(); i > slot; i--)
				copyRecordWithoutLogging(i - 1, i);
			setNumRecordsWithoutLogging(getNumRecords() + 1);
		} finally {
			currentBuff.unlockFlushing();
		}
	}

	/**
	 * Deletes a slot of the current BTreePage. Since this whole action must be
	 * done atomically in a buffer, it will lock the flushing mechanism of the
	 * buffer to ensure no one can flush during the operation.
	 * 
	 * @param slot
	 *            the id of the slot to be deleted
	 */
	public void delete(int slot) {
		currentBuff.lockFlushing();
		try {
			for (int i = slot + 1; i < getNumRecords(); i++)
				copyRecordWithoutLogging(i, i - 1);
			setNumRecordsWithoutLogging(getNumRecords() - 1);
		} finally {
			currentBuff.unlockFlushing();
		}
	}

	/**
	 * Returns true if the block is full.
	 * 
	 * @return true if the block is full
	 */
	public boolean isFull() {
		return slotPosition(getNumRecords() + 1) >= Buffer.BUFFER_SIZE;
	}

	/**
	 * Returns true if the block is going to be full after insertion.
	 * 
	 * @return true if the block is going to be full after insertion
	 */
	public boolean isGettingFull() {
		return slotPosition(getNumRecords() + 2) >= Buffer.BUFFER_SIZE;
	}

	/**
	 * Splits the page at the specified slot. A new page is created, and the
	 * records of the page starting from the split slot are transferred to the
	 * new page.
	 * 
	 * @param splitSlot
	 *            the split position
	 * @param flags
	 *            the flag values
	 * @return the number of the new block
	 */
	public long split(int splitSlot, long[] flags) {
		BlockId newBlk = appendBlock(flags);
		BTreePage newPage = new BTreePage(newBlk, flags.length, schema, tx);
		transferRecords(splitSlot, newPage, 0, getNumRecords() - splitSlot);
		newPage.close();
		return newBlk.number();
	}

	/**
	 * Transfers {@code num} records starting from the {@code start}-th records
	 * of the current page to the {@code destStart}-th slot of the {@code dest}
	 * page.
	 * 
	 * @param start
	 *            the id of the first record to be transfered in the current
	 *            page
	 * @param dest
	 *            the destination page
	 * @param destStart
	 *            the starting slot of the destination page
	 * @param num
	 *            the number of records to be transfered
	 */
	public void transferRecords(int start, BTreePage dest, int destStart, int num) {
		// not deal with the problem that the transfer data is larger than a
		// block
		num = Math.min(getNumRecords() - start, num);

		// Move the records in the destination page in order to clean a space
		for (int i = 0; i < dest.getNumRecords(); i++)
			dest.copyRecord(destStart + i, destStart + num + i);

		// Copy the records from the source page to the destination page
		for (int i = 0; i < num; i++)
			for (String fld : schema.fields())
				dest.setValUnchecked(destStart + i, fld, getVal(start + i, fld));

		// Move the rest records in the source page for deletion
		for (int i = 0; i < getNumRecords() - 1 - num; i++)
			if (start + num + i < getNumRecords())
				copyRecord(start + num + i, start + i);

		// Update the number of records in both pages
		setNumRecords(getNumRecords() - num);
		dest.setNumRecords(dest.getNumRecords() + num);
	}

	public BlockId currentBlk() {
		return blk;
	}

	/**
	 * Returns the number of index records in this page.
	 * 
	 * @return the number of index records in this page
	 */
	public int getNumRecords() {
		// return (Integer) getVal(0, INTEGER).asJavaVal();
		// Optimization:
		if (numberOfRecords == -1)
			numberOfRecords = (Integer) getVal(0, INTEGER).asJavaVal();
		return numberOfRecords;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("{");
		
		// Record count
		int recordCount = getNumRecords();
		sb.append("# of records: ");
		sb.append(recordCount);
		sb.append(", ");
		
		// Flags
		sb.append("flags: [");
		for (int i = 0; i < numberOfFlags; i++) {
			sb.append(getFlag(i));
			if (i < numberOfFlags - 1)
				sb.append(", ");
		}
		sb.append("], ");
		
		// Records
		sb.append("records: [");
		for (int i = 0; i < recordCount; i++) {
			sb.append("{");
			for (String fld : schema.fields()) {
				sb.append(fld);
				sb.append(": ");
				sb.append(getVal(i, fld));
				sb.append(", ");
			}
			sb.delete(sb.length() - 2, sb.length());
			if (i < recordCount - 1)
				sb.append("}, ");
		}
		sb.append("}]}");
		
		return sb.toString();
	}

	private void setNumRecords(int n) {
		Constant v = new IntegerConstant(n);
		setVal(0, v);
		// Optimization:
		numberOfRecords = n;
	}

	private void setNumRecordsWithoutLogging(int n) {
		Constant v = new IntegerConstant(n);
		setValWithoutLogging(0, v);
		// Optimization:
		numberOfRecords = n;
	}

	private void copyRecord(int from, int to) {
		for (String fldname : schema.fields())
			setValUnchecked(to, fldname, getVal(from, fldname));
	}

	private void copyRecordWithoutLogging(int from, int to) {
		for (String fldname : schema.fields())
			setValWithoutLogging(to, fldname, getVal(from, fldname));
	}

	private int fieldPosition(int slot, String fldname) {
		int offset = myOffsetMap.get(fldname);
		return slotPosition(slot) + offset;
	}

	private int slotPosition(int slot) {
		return headerSize + (slot * slotSize);
	}

	private BlockId appendBlock(long[] flags) {
		tx.concurrencyMgr().modifyFile(blk.fileName());
		BTPageFormatter btpf = new BTPageFormatter(schema, flags);
		Buffer buff = tx.bufferMgr().pinNew(blk.fileName(), btpf);
		
		// Danger!
		// Must get block before unpin
		BlockId blk = buff.block();
		tx.bufferMgr().unpin(buff);
		return blk;
	}
	
	private void setValUnchecked(int slot, String fldName, Constant val) {
		Type type = schema.type(fldName);
		Constant v = val.castTo(type);
		setVal(fieldPosition(slot, fldName), v);
	}

	private void setVal(int offset, Constant val) {
		LogSeqNum lsn = tx.recoveryMgr().logSetVal(currentBuff, offset, val);
		currentBuff.setVal(offset, val, tx.getTransactionNumber(), lsn);
	}

	private void setValWithoutLogging(int offset, Constant val) {
		currentBuff.setVal(offset, val, tx.getTransactionNumber(), null);
	}

	private Constant getVal(int offset, Type type) {
		return currentBuff.getVal(offset, type);
	}
}
