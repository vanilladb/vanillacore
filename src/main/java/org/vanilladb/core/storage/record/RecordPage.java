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

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * Manages the placement and access of records in a block.
 */
public class RecordPage implements Record {
	public static final int EMPTY = 0, INUSE = 1;
	public static final int MIN_REC_SIZE = Page.maxSize(INTEGER)
			+ Page.maxSize(BIGINT);
	public static final int FLAG_SIZE = Page.maxSize(INTEGER);
	public static final int MIN_SLOT_SIZE = FLAG_SIZE + MIN_REC_SIZE;

	// Optimization: Materialize the constant value of flag
	private static final IntegerConstant INUSE_CONST = new IntegerConstant(
			INUSE), EMPTY_CONST = new IntegerConstant(EMPTY);

	private Transaction tx;
	private BlockId blk;
	protected TableInfo ti;
	private boolean doLog;

	public Buffer currentBuff;
	private int slotSize;
	private int currentSlot = -1;
	private Map<String, Integer> myOffsetMap;

	// Optimization: Materialize the offset map.
	// /**
	// * Returns the offset of a specified field within a record.
	// *
	// * @param sch
	// * the table's schema
	// * @param fldName
	// * the name of the field
	// *
	// * @return the offset of that field within a record
	// */
	// public static int offset(Schema sch, String fldName) {
	// int pos = 0;
	// for (String fldname : sch.fields()) {
	// if (fldName.equals(fldname))
	// break;
	// pos += Page.maxSize(sch.type(fldname));
	// }
	// return pos;
	// }

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
	 * Returns the number of bytes required to store a record with the specified
	 * schema in disk.
	 * 
	 * @param sch
	 *            the table's schema
	 * @return the size of a record, in bytes
	 */
	public static int recordSize(Schema sch) {
		int pos = 0;
		for (String fldname : sch.fields())
			pos += Page.maxSize(sch.type(fldname));
		return pos < MIN_REC_SIZE ? MIN_REC_SIZE : pos;
	}

	/**
	 * Returns the number of bytes required to store a record slot with the
	 * specified schema in disk.
	 * 
	 * @param sch
	 *            the table's schema
	 * @return the size of a record slot, in bytes
	 */
	public static int slotSize(Schema sch) {
		return recordSize(sch) + Page.maxSize(INTEGER);
	}

	/**
	 * Creates the record manager for the specified block. The current record is
	 * set to be prior to the first one.
	 * 
	 * @param blk
	 *            a block ID
	 * @param ti
	 *            the table's metadata
	 * @param tx
	 *            the transaction
	 * @param doLog
	 *            will it log the modification
	 */
	public RecordPage(BlockId blk, TableInfo ti, Transaction tx, boolean doLog) {
		this.blk = blk;
		this.tx = tx;
		this.ti = ti;
		this.doLog = doLog;
		currentBuff = tx.bufferMgr().pin(blk);

		// Optimization: Reduce the cost of prepare the schema information
		Schema sch = ti.schema();
		int pos = 0;
		myOffsetMap = new HashMap<String, Integer>();
		for (String fldname : sch.fields()) {
			myOffsetMap.put(fldname, pos);
			pos += Page.maxSize(sch.type(fldname));
		}
		pos = pos < MIN_REC_SIZE ? MIN_REC_SIZE : pos;
		slotSize = pos + FLAG_SIZE;
	}

	/**
	 * Closes the manager, by unpinning the block.
	 */
	public void close() {
		if (blk != null) {
			tx.bufferMgr().unpin(currentBuff);
			blk = null;
			currentBuff = null;
		}
	}

	/**
	 * Moves to the next record in the block.
	 * 
	 * @return false if there is no next record.
	 */
	public boolean next() {
		return searchFor(INUSE);
	}

	/**
	 * Returns the value stored in the specified field of this record.
	 * 
	 * @param fldName
	 *            the name of the field.
	 * 
	 * @return the constant stored in that field
	 */
	public Constant getVal(String fldName) {
		int position = fieldPos(fldName);
		return getVal(position, ti.schema().type(fldName));
	}

	/**
	 * Stores a value at the specified field of this record.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @param val
	 *            the constant value stored in that field
	 */
	public void setVal(String fldName, Constant val) {
		int position = fieldPos(fldName);
		setVal(position, val);
	}

	/**
	 * Deletes the current record. Deletion is performed by marking the record
	 * as "deleted" and setting the content as a pointer points to next deleted
	 * slot.
	 * 
	 * @param nextDeletedSlot
	 *            the record is of next deleted slot
	 * 
	 */
	public void delete(RecordId nextDeletedSlot) {
		Constant flag = EMPTY_CONST;
		setVal(currentPos(), flag);
		setNextDeletedSlotId(nextDeletedSlot);
	}
	
	/**
	 * Marks the current slot as in-used.
	 * 
	 * @return true, if it succeed. If the slot has been occupied, return false.
	 */
	public boolean insertIntoTheCurrentSlot() {
		if (!getVal(currentPos(), INTEGER).equals(EMPTY_CONST))
			return false;
		
		setVal(currentPos(), INUSE_CONST);
		return true;
	}

	/**
	 * Inserts a new, blank record somewhere in the page. Return false if there
	 * were no available slots.
	 * 
	 * @return false if the insertion was not possible
	 */
	public boolean insertIntoNextEmptySlot() {
		boolean found = searchFor(EMPTY);
		if (found) {
			Constant flag = INUSE_CONST;
			setVal(currentPos(), flag);
		}
		return found;
	}

	/**
	 * Inserts a new, blank record into this deleted slot and return the record
	 * id of the next one.
	 * 
	 * @return the record id of the next deleted slot
	 */
	public RecordId insertIntoDeletedSlot() {
		RecordId nds = getNextDeletedSlotId();
		// Important: Erase the free chain information.
		// If we didn't do this, it would crash when
		// a tx try to set a VARCHAR at this position
		// since the getVal would get negative size.
		setNextDeletedSlotId(new RecordId(new BlockId("", 0), 0));
		Constant flag = INUSE_CONST;
		setVal(currentPos(), flag);
		return nds;
	}

	/**
	 * Sets the current record to be the record having the specified ID.
	 * 
	 * @param id
	 *            the ID of the record within the page.
	 */
	public void moveToId(int id) {
		currentSlot = id;
	}

	/**
	 * Returns the ID of the current record.
	 * 
	 * @return the ID of the current record
	 */
	public int currentId() {
		return currentSlot;
	}

	/**
	 * Returns the BlockId of the current record.
	 * 
	 * @return the BlockId of the current record
	 */
	public BlockId currentBlk() {
		return blk;
	}

	/**
	 * Print all Slot IN_USE or EMPTY, for debugging
	 */
	public void runAllSlot() {
		moveToId(0);
		System.out.println("== runAllSlot start at " + currentSlot + " ==");
		while (isValidSlot()) {
			if (currentSlot % 10 == 0)
				System.out.print(currentSlot + ": ");
			int flag = (Integer) getVal(currentPos(), INTEGER).asJavaVal();
			System.out.print(flag + " ");
			if ((currentSlot + 1) % 10 == 0)
				System.out.println();
			currentSlot++;
		}
		System.out.println("== runAllSlot end at " + currentSlot + " ==");
	}

	public RecordId getNextDeletedSlotId() {
		int position = currentPos() + FLAG_SIZE;
		long blkNum = (Long) getVal(position, BIGINT).asJavaVal();
		int id = (Integer) getVal(position + Page.maxSize(BIGINT), INTEGER)
				.asJavaVal();
		return new RecordId(new BlockId(blk.fileName(), blkNum), id);
	}

	public void setNextDeletedSlotId(RecordId rid) {
		Constant val = new BigIntConstant(rid.block().number());
		int position = currentPos() + FLAG_SIZE;
		setVal(position, val);
		val = new IntegerConstant(rid.id());
		position += Page.maxSize(BIGINT);
		setVal(position, val);
	}

	private int currentPos() {
		return currentSlot * slotSize;
	}

	private int fieldPos(String fldName) {
		int offset = FLAG_SIZE + myOffsetMap.get(fldName);
		return currentPos() + offset;
	}

	private boolean isValidSlot() {
		return currentPos() + slotSize <= Buffer.BUFFER_SIZE;
	}

	private boolean searchFor(int flag) {
		currentSlot++;
		while (isValidSlot()) {
			if ((Integer) getVal(currentPos(), INTEGER).asJavaVal() == flag) {
				return true;
			}
			currentSlot++;
		}
		return false;
	}

	private Constant getVal(int offset, Type type) {
		if (!isTempTable())
			tx.concurrencyMgr().readRecord(new RecordId(blk, currentSlot));
		return currentBuff.getVal(offset, type);
	}

	private void setVal(int offset, Constant val) {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();
		if (!isTempTable())
			tx.concurrencyMgr().modifyRecord(new RecordId(blk, currentSlot));
		LogSeqNum lsn = doLog ? tx.recoveryMgr().logSetVal(currentBuff, offset, val)
				: null;
		currentBuff.setVal(offset, val, tx.getTransactionNumber(), lsn);
	}

	private boolean isTempTable() {
		return blk.fileName().startsWith("_temp");
	}
}
