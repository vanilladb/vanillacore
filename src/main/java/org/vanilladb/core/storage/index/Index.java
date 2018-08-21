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
package org.vanilladb.core.storage.index;

import org.vanilladb.core.storage.index.btree.BTreeIndex;
import org.vanilladb.core.storage.index.hash.HashIndex;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * An abstract index that defines the index traversal interface and provides
 * type-agnostic methods.
 */
public abstract class Index {

	/**
	 * Estimates the number of block accesses required to find all index records
	 * matching a search range, given the specified numbers of total records and
	 * matching records.
	 * <p>
	 * This number does <em>not</em> include the block accesses required to
	 * retrieve data records.
	 * </p>
	 * 
	 * @param idxType
	 *            the index type
	 * @param keyType
	 *            the type of the search key
	 * @param totRecs
	 *            the total number of records in the table
	 * @param matchRecs
	 *            the number of matching records
	 * @return the estimated the number of block accesses
	 */
	public static long searchCost(IndexType idxType, SearchKeyType keyType, long totRecs, long matchRecs) {
		if (idxType == IndexType.HASH)
			return HashIndex.searchCost(keyType, totRecs, matchRecs);
		else if (idxType == IndexType.BTREE)
			return BTreeIndex.searchCost(keyType, totRecs, matchRecs);
		else
			throw new IllegalArgumentException("unsupported index type");
	}

	public static Index newInstance(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		if (ii.indexType() == IndexType.HASH)
			return new HashIndex(ii, keyType, tx);
		else if (ii.indexType() == IndexType.BTREE)
			return new BTreeIndex(ii, keyType, tx);
		else
			throw new IllegalArgumentException("unsupported index type");
	}

	protected IndexInfo ii;
	protected SearchKeyType keyType;
	protected Transaction tx;
	protected String dataFileName;

	/**
	 * Opens a hash index for the specified index.
	 * 
	 * @param ii
	 *            the information of this index
	 * @param keyType
	 *            the type of the search key
	 * @param tx
	 *            the calling transaction
	 */
	public Index(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		this.ii = ii;
		this.dataFileName = ii.tableName() + ".tbl";
		this.keyType = keyType;
		this.tx = tx;
	}

	/**
	 * Positions the index before the first index record matching the specified
	 * range of search keys.
	 * 
	 * @param searchRange
	 *            the range of search keys
	 */
	public abstract void beforeFirst(SearchRange searchRange);

	/**
	 * Moves the index to the next record matching the search range specified in
	 * the {@link #beforeFirst} method. Returns false if there are no more such
	 * index records.
	 * 
	 * @return false if no other index records for the search range.
	 */
	public abstract boolean next();

	/**
	 * Returns the data record ID stored in the current index record.
	 * 
	 * @return the data record ID stored in the current index record.
	 */
	public abstract RecordId getDataRecordId();

	/**
	 * Inserts an index record having the specified key and data record ID.
	 * 
	 * @param key
	 *            the key in the new index record.
	 * @param dataRecordId
	 *            the data record ID in the new index record.
	 * @param doLogicalLogging
	 *            is logical logging enabled
	 */
	public abstract void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging);

	/**
	 * Deletes the index record having the specified key and data record ID.
	 * 
	 * @param key
	 *            the key of the deleted index record
	 * @param dataRecordId
	 *            the data record ID of the deleted index record
	 * @param doLogicalLogging
	 *            is logical logging enabled
	 */
	public abstract void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging);

	/**
	 * Closes the index.
	 */
	public abstract void close();

	/**
	 * Preload the index blocks to memory.
	 */
	public abstract void preLoadToMemory();

	public IndexInfo getIndexInfo() {
		return ii;
	}

	public SearchKeyType getKeyType() {
		return keyType;
	}
}
