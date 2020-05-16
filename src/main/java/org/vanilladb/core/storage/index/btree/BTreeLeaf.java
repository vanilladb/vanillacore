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

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;

/**
 * A B-tree leaf page that iterates over the B-tree leaf blocks in a file.
 * <p>
 * There are two flags in each B-tree leaf block. The first flag is a pointer
 * (block number) to the next overflow block. For a regular block without any
 * overflow page, this flag is set to -1. If the page is the last overflow
 * block, then this flag points circularly to the regular block. The second flag
 * is a pointer to the next sibling (regular) block. The value of this flag is
 * undefined if the block is an overflow page.
 * </p>
 * <p>
 * Note that currently there will be some "dead blocks" in the file that are
 * empty but can never be used. These dead blocks are caused by empty overflow
 * pages after {@link #delete(int) deletion}. On way to fix this problem is to
 * maintain a list of dead blocks in the file and to reuse them during
 * {@link #insert insertion}. This is left as future work.
 * </p>
 */
public class BTreeLeaf {
	/**
	 * A field name of the schema of B-tree leaf records.
	 */
	static final String SCH_KEY = "key", SCH_RID_BLOCK = "block", SCH_RID_ID = "id";

	static int NUM_FLAGS = 2;
	
	private static final String FILENAME_POSTFIX = "_leaf.idx";
	
	public static void insertASlot(Transaction tx, BlockId blk, SearchKeyType keyType, int slotId) {
		// Open the specified leaf
		BTreeLeaf dir = new BTreeLeaf(blk, keyType, tx);

		// Insert the specified slot
		dir.currentPage.insert(slotId);

		// Close the directory
		dir.close();
	}
	
	public static void deleteASlot(Transaction tx, BlockId blk, SearchKeyType keyType, int slotId) {
		// Open the specified leaf
		BTreeLeaf dir = new BTreeLeaf(blk, keyType, tx);

		// Delete the specified slot
		dir.currentPage.delete(slotId);

		// Close the directory
		dir.close();
	}
	
	public static String getFileName(String indexName) {
		return indexName + FILENAME_POSTFIX;
	}
	
	static String keyFieldName(int index) {
		return SCH_KEY + index;
	}

	/**
	 * Returns the schema of the B-tree leaf records.
	 * 
	 * @param keyType
	 *            the type of the indexed field
	 * 
	 * @return the schema of the index records
	 */
	static Schema schema(SearchKeyType keyType) {
		Schema sch = new Schema();
		for (int i = 0; i < keyType.length(); i++)
			sch.addField(keyFieldName(i), keyType.get(i));
		sch.addField(SCH_RID_BLOCK, BIGINT);
		sch.addField(SCH_RID_ID, INTEGER);
		return sch;
	}

	static long getOverflowFlag(BTreePage p) {
		return p.getFlag(0);
	}

	static void setOverflowFlag(BTreePage p, long val) {
		p.setFlag(0, val);
	}

	static long getSiblingFlag(BTreePage p) {
		return p.getFlag(1);
	}

	static void setSiblingFlag(BTreePage p, long val) {
		p.setFlag(1, val);
	}

	static SearchKey getKey(BTreePage p, int slot, int keyLen) {
		Constant[] vals = new Constant[keyLen];
		for (int i = 0; i < keyLen; i++)
			vals[i] = p.getVal(slot, keyFieldName(i));
		return new SearchKey(vals);
	}

	static RecordId getDataRecordId(BTreePage p, int slot, String dataFileName) {
		long blkNum = (Long) p.getVal(slot, SCH_RID_BLOCK).asJavaVal();
		int id = (Integer) p.getVal(slot, SCH_RID_ID).asJavaVal();
		return new RecordId(new BlockId(dataFileName, blkNum), id);
	}

	private Schema schema;
	private SearchKeyType keyType;
	private Transaction tx;
	private ConcurrencyMgr ccMgr;
	private SearchRange searchRange;
	private String dataFileName;

	private BTreePage currentPage;
	private int currentSlot;

	private boolean isOverflowing;
	private long overflowFrom = -1;

	private long moveFrom = -1;

	/**
	 * Opens a page to hold the specified B-tree leaf block. The page is
	 * positioned immediately before the first B-tree leaf record matching the
	 * specified search range (if any).
	 * 
	 * @param dataFileName
	 *            the data file name
	 * @param blk
	 *            a block ID
	 * @param keyType
	 *            the type of the search key
	 * @param searchRange
	 *            the range of search keys
	 * @param tx
	 *            the calling transaction
	 */
	public BTreeLeaf(String dataFileName, BlockId blk, SearchKeyType keyType,
			SearchRange searchRange, Transaction tx) {
		this.dataFileName = dataFileName;
		this.schema = schema(keyType);
		this.keyType = keyType;
		this.searchRange = searchRange;
		this.tx = tx;
		this.currentPage = new BTreePage(blk, NUM_FLAGS, schema, tx);
		ccMgr = tx.concurrencyMgr();
		moveSlotBefore();
	}
	
	/**
	 * Opens a b-tree leaf page. If a b-tree leaf page was opened by this method,
	 * it could not be used for searching since the search range was not given.
	 * 
	 * @param indexName
	 * @param keyType
	 * @param blkNum
	 * @param tx
	 */
	private BTreeLeaf(BlockId blk, SearchKeyType keyType, Transaction tx) {
		this.dataFileName = null;
		this.schema = schema(keyType);
		this.keyType = keyType;
		this.searchRange = null;
		this.tx = tx;
		this.currentPage = new BTreePage(blk, NUM_FLAGS, schema, tx);
		ccMgr = tx.concurrencyMgr();
	}

	/**
	 * Closes the leaf page.
	 */
	public void close() {
		currentPage.close();
	}

	/**
	 * Moves to the next B-tree leaf record matching the search key.
	 * 
	 * @return false if there are no more leaf records for the search key
	 */
	public boolean next() {
		while (true) {
			currentSlot++;
			if (!isOverflowing) { // not in an overflow block
				// if it reached the end of the block
				if (currentSlot >= currentPage.getNumRecords()) {
					if (getSiblingFlag(currentPage) != -1) {
						moveTo(getSiblingFlag(currentPage), -1);
						continue;
					}
					return false;
					// if the key of this slot match what we want
				} else if (searchRange.match(getKey(currentPage, currentSlot, keyType.length()))) {
					/*
					 * Move to records in overflow blocks first. An overflow block
					 * cannot be empty.
					 */
					if (currentSlot == 0 && getOverflowFlag(currentPage) != -1) {
						isOverflowing = true;
						overflowFrom = currentPage.currentBlk().number();
						moveTo(getOverflowFlag(currentPage), 0);
					}
					return true;
				} else if (searchRange.betweenMinAndMax(getKey(currentPage, currentSlot, keyType.length()))) {
					continue;
				} else
					return false;
			} else { // in an overflow block
				// All the records in an overflow block have the same key
				// so that we do not need to check the key in an overflow block.
				
				if (currentSlot >= currentPage.getNumRecords()) {
					moveTo(getOverflowFlag(currentPage), 0);
					/*
					 * Move back to the first record in the regular block finally.
					 */
					if (currentPage.currentBlk().number() == overflowFrom) {
						isOverflowing = false;
						overflowFrom = -1;
					}
				}
				
				return true;
			}
		}
	}

	/**
	 * Returns the data record ID of the current B-tree leaf record.
	 * 
	 * @return the data record ID of the current record
	 */
	public RecordId getDataRecordId() {
		return getDataRecordId(currentPage, currentSlot, dataFileName);
	}

	/**
	 * Inserts a new B-tree leaf record having the specified data record ID and
	 * the previously-specified search key. This method can only be called once,
	 * immediately after construction.
	 * 
	 * @param dataRecordId
	 *            the data record ID of the new record
	 * @return the directory entry of the newly-split page or null of there is
	 *         no split
	 */
	public DirEntry insert(RecordId dataRecordId) {
		// search range must be a constant
		if (!searchRange.isSingleValue())
			throw new IllegalStateException();
		
		// ccMgr.modifyLeafBlock(currentPage.currentBlk());
		currentSlot++;
		SearchKey searchKey = searchRange.asSearchKey();
		insert(currentSlot, searchKey, dataRecordId);
		
		/*
		 * If the inserted key is less than the key stored in the overflow
		 * blocks, split this block to make sure that the key of the first
		 * record in every block will be the same as the key of records in
		 * the overflow blocks.
		 */
		if (currentSlot == 0 && getOverflowFlag(currentPage) != -1 &&
				!getKey(currentPage, 1, keyType.length()).equals(searchKey)) {
			SearchKey splitKey = getKey(currentPage, 1, keyType.length());
			long newBlkNum = currentPage.split(1,
					new long[] { getOverflowFlag(currentPage),
							getSiblingFlag(currentPage) });
			setOverflowFlag(currentPage, -1);
			setSiblingFlag(currentPage, newBlkNum);
			return new DirEntry(splitKey, newBlkNum);
		}

		if (!currentPage.isFull())
			return null;
		
		/*
		 * If this block is full, then split the block and return the directory
		 * entry of the new block.
		 */
		SearchKey firstKey = getKey(currentPage, 0, keyType.length());
		SearchKey lastKey = getKey(currentPage, currentPage.getNumRecords() - 1,
				keyType.length());
		if (lastKey.equals(firstKey)) {
			/*
			 * If all of the records in the page have the same key, then the
			 * block does not split; instead, all but one of the records are
			 * placed into an overflow block.
			 */
			long overflowFlag = (getOverflowFlag(currentPage) == -1) ?
					currentPage.currentBlk().number() : getOverflowFlag(currentPage);
			long newBlkNum = currentPage.split(1, new long[] { overflowFlag, -1 });
			setOverflowFlag(currentPage, newBlkNum);
			return null;
			
		} else {
			int splitPos = currentPage.getNumRecords() / 2;
			SearchKey splitKey = getKey(currentPage, splitPos, keyType.length());
			
			// records having the same key must be in the same block
			if (splitKey.equals(firstKey)) {
				// move right, looking for a different key
				while (getKey(currentPage, splitPos, keyType.length()).equals(splitKey))
					splitPos++;
				splitKey = getKey(currentPage, splitPos, keyType.length());
			} else {
				// move left, looking for first entry having that key
				while (getKey(currentPage, splitPos - 1, keyType.length())
						.equals(splitKey))
					splitPos--;
			}
			
			// split the block
			long newBlkNum = currentPage.split(splitPos, new long[] { -1,
					getSiblingFlag(currentPage) });
			setSiblingFlag(currentPage, newBlkNum);
			return new DirEntry(splitKey, newBlkNum);
		}
	}

	/**
	 * Deletes the B-tree leaf record having the specified data record ID and
	 * the previously-specified search key. This method can only be called once,
	 * immediately after construction.
	 * 
	 * @param dataRecordId
	 *            the data record ID whose record is to be deleted
	 */
	public void delete(RecordId dataRecordId) {
		// search range must be a constant
		if (!searchRange.isSingleValue())
			throw new IllegalStateException();

		// delete all entry with the specific key
		while (next())
			if (getDataRecordId().equals(dataRecordId)) {
				// ccMgr.modifyLeafBlock(currentPage.currentBlk());
				delete(currentSlot);
				break;
			}

		if (!isOverflowing) {
			/*
			 * If the current regular block is empty or the key of the first
			 * record is not equal to that of records in overflow blocks,
			 * transfer one record from a overflow block to here (if any).
			 */
			if (getOverflowFlag(currentPage) != -1) {
				// get overflow page
				BlockId blk = new BlockId(currentPage.currentBlk().fileName(), getOverflowFlag(currentPage));
				ccMgr.modifyLeafBlock(blk);
				BTreePage overflowPage = new BTreePage(blk, NUM_FLAGS, schema, tx);

				SearchKey firstKey = getKey(currentPage, 0, keyType.length());
				if ((currentPage.getNumRecords() == 0
						|| (overflowPage.getNumRecords() != 0
						&& getKey(overflowPage, 0, keyType.length()) != firstKey))) {
					overflowPage.transferRecords(overflowPage.getNumRecords() - 1,
							currentPage, 0, 1);
					// if the overflow block is empty, make it a dead block
					if (overflowPage.getNumRecords() == 0) {
						long overflowFlag = (getOverflowFlag(overflowPage)
								== currentPage.currentBlk().number())
								? -1 : getOverflowFlag(overflowPage);
						setOverflowFlag(currentPage, overflowFlag);
					}
					overflowPage.close();
				}
			}
		} else {
			/*
			 * If the current overflow block is empty, make it a dead block.
			 */
			if (currentPage.getNumRecords() == 0) {
				// reset the overflow flag of original page
				BlockId blk = new BlockId(currentPage.currentBlk().fileName(), moveFrom);
				// ccMgr.modifyLeafBlock(blk);
				BTreePage prePage = new BTreePage(blk, NUM_FLAGS, schema, tx);
				long overflowFlag = (getOverflowFlag(currentPage) == prePage.currentBlk().number()) ? -1
						: getOverflowFlag(currentPage);
				setOverflowFlag(prePage, overflowFlag);
				prePage.close();
			}
		}
	}
	
	public int getNumRecords() {
		return currentPage.getNumRecords();
	}

	/**
	 * Positions the current slot right before the first index record that matches
	 * the specified search range.
	 */
	private void moveSlotBefore() {
		/*
		 * int slot = 0; while (slot < currentPage.getNumRecords() &&
		 * searchRange.largerThan(getKey(currentPage, slot))) slot++;
		 * 
		 * currentSlot = slot - 1;
		 */
		// Optimization: Use binary search rather than sequential search
		int startSlot = 0, endSlot = currentPage.getNumRecords() - 1;
		int middleSlot = (startSlot + endSlot) / 2;
		SearchKey searchMin = searchRange.getMin();

		if (endSlot >= 0) {
			while (middleSlot != startSlot) {
				if (searchMin.compareTo(
						getKey(currentPage, middleSlot, keyType.length())) > 0)
					startSlot = middleSlot;
				else
					endSlot = middleSlot;

				middleSlot = (startSlot + endSlot) / 2;
			}
			
			if (searchMin.compareTo(
					getKey(currentPage, endSlot, keyType.length())) > 0)
				currentSlot = endSlot;
			else if (searchMin.compareTo(
					getKey(currentPage, startSlot, keyType.length())) > 0)
				currentSlot = startSlot;
			else
				currentSlot = startSlot - 1;
		} else
			currentSlot = -1;
	}
	
	/**
	 * Opens the page for the specified block and moves the current slot to the
	 * specified position.
	 */
	private void moveTo(long blkNum, int slot) {
		moveFrom = currentPage.currentBlk().number(); // for deletion
		BlockId blk = new BlockId(currentPage.currentBlk().fileName(), blkNum);
		ccMgr.readLeafBlock(blk);
		currentPage.close();
		currentPage = new BTreePage(blk, NUM_FLAGS, schema, tx);
		currentSlot = slot;
	}
	
	private void insert(int slot, SearchKey key, RecordId rid) {
		// Insert an entry to the page
		tx.recoveryMgr().logIndexPageInsertion(currentPage.currentBlk(), false, keyType, slot);
		currentPage.insert(slot);
		
		for (int i = 0; i < keyType.length(); i++)
			currentPage.setVal(slot, keyFieldName(i), key.get(i));
		currentPage.setVal(slot, SCH_RID_BLOCK, new BigIntConstant(rid.block().number()));
		currentPage.setVal(slot, SCH_RID_ID, new IntegerConstant(rid.id()));
	}
	
	private void delete(int slot) {
		// Delete an entry of the page
		tx.recoveryMgr().logIndexPageDeletion(currentPage.currentBlk(), false, keyType, slot);
		currentPage.delete(slot);
	}
}
