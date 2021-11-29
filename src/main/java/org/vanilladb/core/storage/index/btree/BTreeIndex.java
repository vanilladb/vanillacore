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

import java.util.List;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;

/**
 * A B-tree implementation of {@link Index}.
 */
public class BTreeIndex extends Index {
	protected static enum SearchPurpose { READ, INSERT, DELETE };
	private ConcurrencyMgr ccMgr;
	private String leafFileName, dirFileName;
	private BTreeLeaf leaf = null;
	private BlockId rootBlk;
	private List<BlockId> dirsMayBeUpdated;
	private boolean isBeforeFirsted;

	public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
		int dirRpb = Buffer.BUFFER_SIZE / BTreePage.slotSize(BTreeDir.schema(keyType));
		int leafRpb = Buffer.BUFFER_SIZE / BTreePage.slotSize(BTreeLeaf.schema(keyType));
		long leafs = (int) Math.ceil((double) totRecs / leafRpb);
		long matchLeafs = (int) Math.ceil((double) matchRecs / leafRpb);
		return (long) Math.ceil(Math.log(leafs) / Math.log(dirRpb)) + matchLeafs;
	}

	/**
	 * Opens a B-tree index for the specified index. The method determines the
	 * appropriate files for the leaf and directory records, creating them if
	 * they did not exist.
	 * 
	 * @param ii
	 *            the information of this index
	 * @param keyType
	 *            the type of the indexed field
	 * @param tx
	 *            the calling transaction
	 */
	public BTreeIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		super(ii, keyType, tx);
		
		this.ccMgr = tx.concurrencyMgr();
		
		// Initialize the first leaf block (if it needed)
		leafFileName = BTreeLeaf.getFileName(ii.indexName());
		if (isFileEmpty(leafFileName))
			appendBlock(leafFileName, BTreeLeaf.schema(keyType), new long[] { -1, -1 });

		// Initialize the first directory block (if it needed)
		dirFileName = BTreeDir.getFileName(ii.indexName());
		rootBlk = new BlockId(dirFileName, 0);
		if (isFileEmpty(dirFileName))
			appendBlock(dirFileName, BTreeDir.schema(keyType), new long[] { 0 });
		
		// Insert an initial directory entry (if it needed)
		BTreeDir rootDir = new BTreeDir(rootBlk, keyType, tx);
		if (rootDir.getNumRecords() == 0)
			rootDir.insert(new DirEntry(keyType.getMin(), 0));
		rootDir.close();
	}

	@Override
	public void preLoadToMemory() {

		// Read all blocks of the directory file
		long dirSize = fileSize(dirFileName);
		BlockId blk;
		for (int i = 0; i < dirSize; i++) {
			blk = new BlockId(dirFileName, i);
			tx.bufferMgr().pin(blk);
		}

		// Read all blocks of the leaf file
		long leafSize = fileSize(leafFileName);
		for (int i = 0; i < leafSize; i++) {
			blk = new BlockId(leafFileName, i);
			tx.bufferMgr().pin(blk);
		}
	}

	/**
	 * Traverses the directory to find the leaf page corresponding to the lower
	 * bound of the specified key range. The method then position the page
	 * before the first record (if any) matching the that lower bound. The leaf
	 * page is kept open, for use by the methods {@link #next} and
	 * {@link #getDataRecordId}.
	 * 
	 * @see Index#beforeFirst
	 */
	@Override
	public void beforeFirst(SearchRange searchRange) {
		if (!searchRange.isValid())
			return;

		search(searchRange, SearchPurpose.READ);
		isBeforeFirsted = true;
	}

	/**
	 * Moves to the next index record in B-tree leaves matching the
	 * previously-specified search key. Returns false if there are no more such
	 * records.
	 * 
	 * @see Index#next
	 */
	@Override
	public boolean next() {
		if (!isBeforeFirsted)
			throw new IllegalStateException("You must call beforeFirst() before iterating index '"
					+ ii.indexName() + "'");
		
		return leaf == null ? false : leaf.next();
	}

	/**
	 * Returns the data record ID from the current index record in B-tree
	 * leaves.
	 * 
	 * @see Index#getDataRecordId()
	 */
	@Override
	public RecordId getDataRecordId() {
		return leaf.getDataRecordId();
	}

	/**
	 * Inserts the specified record into the index. The method first traverses
	 * the directory to find the appropriate leaf page; then it inserts the
	 * record into the leaf. If the insertion causes the leaf to split, then the
	 * method calls insert on the root, passing it the directory entry of the
	 * new leaf page. If the root node splits, then {@link BTreeDir#makeNewRoot}
	 * is called.
	 * 
	 * @see Index#insert(SearchKey, RecordId, boolean)
	 */
	@Override
	public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		if (tx.isReadOnly())
			throw new UnsupportedOperationException();

		// search leaf block for insertion
		search(new SearchRange(key), SearchPurpose.INSERT);
		DirEntry newEntry = leaf.insert(dataRecordId);
		leaf.close();
		if (newEntry == null)
			return;
		
		// log the logical operation starts
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();

		// insert the directory entry from the lowest directory
		for (int i = dirsMayBeUpdated.size() - 1; i >= 0; i--) {
			BlockId dirBlk = dirsMayBeUpdated.get(i);
			BTreeDir dir = new BTreeDir(dirBlk, keyType, tx);
			newEntry = dir.insert(newEntry);
			dir.close();
			if (newEntry == null)
				break;
		}
		if (newEntry != null) {
			BTreeDir root = new BTreeDir(rootBlk, keyType, tx);
			root.makeNewRoot(newEntry);
			root.close();
		}
		dirsMayBeUpdated = null;
		
		// log the logical operation ends
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
					dataRecordId.block().number(), dataRecordId.id());
	}

	/**
	 * Deletes the specified index record. The method first traverses the
	 * directory to find the leaf page containing that record; then it deletes
	 * the record from the page.
	 * 
	 * @see Index#delete(SearchKey, RecordId, boolean)
	 */
	@Override
	public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		if (tx.isReadOnly())
			throw new UnsupportedOperationException();

		search(new SearchRange(key), SearchPurpose.DELETE);
		
		// log the logical operation starts
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();
		
		leaf.delete(dataRecordId);
		
		// log the logical operation ends
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key,
					dataRecordId.block().number(), dataRecordId.id());
	}

	/**
	 * Closes the index by closing its open leaf page, if necessary.
	 * 
	 * @see Index#close()
	 */
	@Override
	public void close() {
		if (leaf != null) {
			leaf.close();
			leaf = null;
		}
		dirsMayBeUpdated = null;
	}

	private void search(SearchRange searchRange, SearchPurpose purpose) {
		close();
		BlockId leafblk;
		BTreeDir root = new BTreeDir(rootBlk, keyType, tx);
		leafblk = root.search(searchRange.getMin(), leafFileName, purpose);

		// get the dir list for update
		if (purpose == SearchPurpose.INSERT)
			dirsMayBeUpdated = root.dirsMayBeUpdated();
		root.close();

		// read leaf block
		leaf = new BTreeLeaf(dataFileName, leafblk, keyType, searchRange, tx);
	}

	private boolean isFileEmpty(String fileName) {
		// Optimization
		// Assume we won't delete the BtreeIndex.
		// Once the index file is not empty, the file won't be empty again
		ccMgr.readFile(fileName);
		return VanillaDb.fileMgr().isFileEmpty(fileName);
		
	}
	
	private long fileSize(String fileName) {
		ccMgr.readFile(fileName);
		return VanillaDb.fileMgr().size(fileName);
	}
	
	private BlockId appendBlock(String fileName, Schema sch, long[] flags) {
		ccMgr.modifyFile(fileName);
		BTPageFormatter btpf = new BTPageFormatter(sch, flags);

		Buffer buff = tx.bufferMgr().pinNew(fileName, btpf);
		// Danger!
		// Must get block before unpin
		BlockId blk = buff.block();
		tx.bufferMgr().unpin(buff);
		return blk;
	}
}
