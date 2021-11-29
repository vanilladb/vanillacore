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

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.SchemaIncompatibleException;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * Manages a file of records. There are methods for iterating through the
 * records and accessing their contents. Note that the first block (block 0) of
 * a record file is reserved for file header, and the actual data block is start
 * from block 1.
 * 
 * <p>
 * The {@link #insert()} method must be called before setters.
 * </p>
 * 
 * <p>
 * The {@link #beforeFirst()} method must be called before {@link #next()}.
 * </p>
 */
public class RecordFile implements Record {
	private BlockId headerBlk;
	private TableInfo ti;
	private Transaction tx;
	private String fileName;
	private RecordPage rp;
	private FileHeaderPage fhp;
	private long currentBlkNum;
	private boolean doLog;
	private boolean isBeforeFirsted;

	/**
	 * Constructs an object to manage a file of records. If the file does not
	 * exist, it is created. This method should be called by {@link TableInfo}
	 * only. To obtain an instance of this class, call
	 * {@link TableInfo#open(Transaction, boolean)} instead.
	 * 
	 * @param ti
	 *            the table metadata
	 * @param tx
	 *            the transaction
	 * @param doLog
	 *            true if the underlying record modification should perform
	 *            logging
	 */
	public RecordFile(TableInfo ti, Transaction tx, boolean doLog) {
		this.ti = ti;
		this.tx = tx;
		this.doLog = doLog;
		fileName = ti.fileName();
		headerBlk = new BlockId(fileName, 0);
	}

	/**
	 * Format the header of specified file.
	 * 
	 * @param fileName
	 *            the file name
	 * @param tx
	 *            the transaction
	 */
	public static void formatFileHeader(String fileName, Transaction tx) {
		tx.concurrencyMgr().modifyFile(fileName);
		// header should be the first block of the given file
		if (VanillaDb.fileMgr().size(fileName) == 0) {
			FileHeaderFormatter fhf = new FileHeaderFormatter();
			Buffer buff = tx.bufferMgr().pinNew(fileName, fhf);
			tx.bufferMgr().unpin(buff);
		}
	}

	/**
	 * Closes the record file.
	 */
	public void close() {
		if (rp != null)
			rp.close();
		if (fhp != null)
			closeHeader();
	}

	/**
	 * Remove the record file.
	 * TODO: handle the concurrency issues that might happen
	 */
	public void remove() {
		close();
		VanillaDb.fileMgr().delete(fileName);
	}

	/**
	 * Positions the current record so that a call to method next will wind up
	 * at the first record.
	 */
	public void beforeFirst() {
		close();
		currentBlkNum = 0; // first data block is block 1
		isBeforeFirsted = true;
	}

	/**
	 * Moves to the next record. Returns false if there is no next record.
	 * 
	 * @return false if there is no next record.
	 */
	public boolean next() {
		if (!isBeforeFirsted)
			throw new IllegalStateException("You must call beforeFirst() before iterating table '"
					+ ti.tableName() + "'");
		
		if (currentBlkNum == 0 && !moveTo(1))
			return false;
		while (true) {
			if (rp.next())
				return true;
			if (!moveTo(currentBlkNum + 1))
				return false;
		}
	}

	/**
	 * Returns the value of the specified field in the current record. Getter
	 * should be called after {@link #next()} or {@link #moveToRecordId(RecordId)}.
	 * 
	 * @param fldName
	 *            the name of the field
	 * 
	 * @return the value at that field
	 */
	public Constant getVal(String fldName) {
		return rp.getVal(fldName);
	}

	/**
	 * Sets a value of the specified field in the current record. The type of
	 * the value must be equal to that of the specified field.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @param val
	 *            the new value for the field
	 */
	public void setVal(String fldName, Constant val) {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();
		Type fldType = ti.schema().type(fldName);

		Constant v = val.castTo(fldType);
		if (Page.size(v) > Page.maxSize(fldType))
			throw new SchemaIncompatibleException();
		rp.setVal(fldName, v);
	}

	/**
	 * Deletes the current record. The client must call next() to move to the
	 * next record. Calls to methods on a deleted record have unspecified
	 * behavior.
	 */
	public void delete() {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();

		if (fhp == null)
			fhp = openHeaderForModification();

		// Log that this logical operation starts
		RecordId deletedRid = currentRecordId();
		tx.recoveryMgr().logLogicalStart();

		// Delete the current record
		rp.delete(fhp.getLastDeletedSlot());
		fhp.setLastDeletedSlot(currentRecordId());

		// Log that this logical operation ends
		tx.recoveryMgr().logRecordFileDeletionEnd(ti.tableName(), deletedRid.block().number(), deletedRid.id());

		// Close the header (release the header lock)
		closeHeader();
	}

	/**
	 * Deletes the specified record.
	 * 
	 * @param rid
	 *            the record to be deleted
	 */
	public void delete(RecordId rid) {
		// Note that the delete() method will
		// take care the concurrency and recovery problems
		moveToRecordId(rid);
		delete();
	}

	/**
	 * Inserts a new, blank record somewhere in the file beginning at the
	 * current record. If the new record does not fit into an existing block,
	 * then a new block is appended to the file.
	 */
	public void insert() {
		// Block read-only transaction
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();

		// Insertion may change the properties of this file,
		// so that we need to lock the file.
		if (!isTempTable())
			tx.concurrencyMgr().modifyFile(fileName);

		// Modify the free chain which is start from a pointer in
		// the header of the file.
		if (fhp == null)
			fhp = openHeaderForModification();

		// Log that this logical operation starts
		tx.recoveryMgr().logLogicalStart();

		if (fhp.hasDeletedSlots()) {
			// Insert into a deleted slot
			moveToRecordId(fhp.getLastDeletedSlot());
			RecordId lds = rp.insertIntoDeletedSlot();
			fhp.setLastDeletedSlot(lds);
		} else {
			// Insert into a empty slot
			if (!fhp.hasDataRecords()) { // no record inserted before
				// Create the first data block
				appendBlock();
				moveTo(1);
				rp.insertIntoNextEmptySlot();
			} else {
				// Find the tail page
				RecordId tailSlot = fhp.getTailSolt();
				moveToRecordId(tailSlot);
				while (!rp.insertIntoNextEmptySlot()) {
					if (atLastBlock())
						appendBlock();
					moveTo(currentBlkNum + 1);
				}
			}
			fhp.setTailSolt(currentRecordId());
		}

		// Log that this logical operation ends
		RecordId insertedRid = currentRecordId();
		tx.recoveryMgr().logRecordFileInsertionEnd(ti.tableName(), insertedRid.block().number(), insertedRid.id());

		// Close the header (release the header lock)
		closeHeader();
	}

	/**
	 * Inserts a record to a specified physical address.
	 * 
	 * @param rid
	 *            the address a record will be inserted
	 */
	public void insert(RecordId rid) {
		// Block read-only transaction
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();

		// Insertion may change the properties of this file,
		// so that we need to lock the file.
		if (!isTempTable())
			tx.concurrencyMgr().modifyFile(fileName);

		// Open the header
		if (fhp == null)
			fhp = openHeaderForModification();

		// Log that this logical operation starts
		tx.recoveryMgr().logLogicalStart();

		// Mark the specified slot as in used
		moveToRecordId(rid);
		if (!rp.insertIntoTheCurrentSlot())
			throw new RuntimeException("the specified slot: " + rid + " is in used");

		// Traverse the free chain to find the specified slot
		RecordId lastSlot = null;
		RecordId currentSlot = fhp.getLastDeletedSlot();
		while (!currentSlot.equals(rid) && currentSlot.block().number() != FileHeaderPage.NO_SLOT_BLOCKID) {
			moveToRecordId(currentSlot);
			lastSlot = currentSlot;
			currentSlot = rp.getNextDeletedSlotId();
		}

		// Remove the specified slot from the chain
		// If it is the first slot
		if (lastSlot == null) {
			moveToRecordId(currentSlot);
			fhp.setLastDeletedSlot(rp.getNextDeletedSlotId());

			// If it is in the middle
		} else if (currentSlot.block().number() != FileHeaderPage.NO_SLOT_BLOCKID) {
			moveToRecordId(currentSlot);
			RecordId nextSlot = rp.getNextDeletedSlotId();
			moveToRecordId(lastSlot);
			rp.setNextDeletedSlotId(nextSlot);
		}

		// Log that this logical operation ends
		tx.recoveryMgr().logRecordFileInsertionEnd(ti.tableName(), rid.block().number(), rid.id());

		// Close the header (release the header lock)
		closeHeader();
	}

	/**
	 * Positions the current record as indicated by the specified record ID .
	 * 
	 * @param rid
	 *            a record ID
	 */
	public void moveToRecordId(RecordId rid) {
		moveTo(rid.block().number());
		rp.moveToId(rid.id());
	}

	/**
	 * Returns the record ID of the current record.
	 * 
	 * @return a record ID
	 */
	public RecordId currentRecordId() {
		int id = rp.currentId();
		return new RecordId(new BlockId(fileName, currentBlkNum), id);
	}

	/**
	 * Returns the number of blocks in the specified file. This method first
	 * calls corresponding concurrency manager to guarantee the isolation
	 * property, before asking the file manager to return the file size.
	 * 
	 * @return the number of blocks in the file
	 */
	public long fileSize() {
		if (!isTempTable())
			tx.concurrencyMgr().readFile(fileName);
		return VanillaDb.fileMgr().size(fileName);
	}

	private boolean moveTo(long b) {
		if (rp != null)
			rp.close();
		
		if (b >= fileSize()) // block b not allocated yet
			return false;
		currentBlkNum = b;
		BlockId blk = new BlockId(fileName, currentBlkNum);
		rp = new RecordPage(blk, ti, tx, doLog);
		return true;
	}

	private void appendBlock() {
		if (!isTempTable())
			tx.concurrencyMgr().modifyFile(fileName);
		RecordFormatter fmtr = new RecordFormatter(ti);
		Buffer buff = tx.bufferMgr().pinNew(fileName, fmtr);
		// Danger!
		// Must get block before unpin
		if (!isTempTable())
			tx.concurrencyMgr().insertBlock(buff.block());
		tx.bufferMgr().unpin(buff);	
	}

	private FileHeaderPage openHeaderForModification() {
		// acquires exclusive access to the header
		if (!isTempTable())
			tx.concurrencyMgr().lockRecordFileHeader(headerBlk);
		return new FileHeaderPage(fileName, tx);
	}

	private void closeHeader() {
		// Release the lock of the header
		if (fhp != null) {
			tx.concurrencyMgr().releaseRecordFileHeader(headerBlk);
			fhp = null;
		}
	}

	private boolean isTempTable() {
		return fileName.startsWith("_temp");
	}

	private boolean atLastBlock() {
		return currentBlkNum == fileSize() - 1;
	}
}
