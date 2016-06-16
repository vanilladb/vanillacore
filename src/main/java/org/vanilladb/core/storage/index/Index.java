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
package org.vanilladb.core.storage.index;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Type;
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
	 * A supported index type.
	 */
	public static final int IDX_HASH = 0, IDX_BTREE = 1;

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
	 * @param fldType
	 *            the type of the indexed field
	 * @param totRecs
	 *            the total number of records in the table
	 * @param matchRecs
	 *            the number of matching records
	 * @return the estimated the number of block accesses
	 */
	public static long searchCost(int idxType, Type fldType, long totRecs,
			long matchRecs) {
		if (idxType == IDX_HASH)
			return HashIndex.searchCost(fldType, totRecs, matchRecs);
		else if (idxType == IDX_BTREE)
			return BTreeIndex.searchCost(fldType, totRecs, matchRecs);
		else
			throw new IllegalArgumentException("unsupported index type");
	}

	public static Index newInstance(IndexInfo ii, Type fldType, Transaction tx) {
		if (ii.indexType() == IDX_HASH)
			return new HashIndex(ii, fldType, tx);
		else if (ii.indexType() == IDX_BTREE)
			return new BTreeIndex(ii, fldType, tx);
		else
			throw new IllegalArgumentException("unsupported index type");
	}

	/**
	 * Positions the index before the first index record matching the specified
	 * range of search keys.
	 * 
	 * @param searchRange
	 *            the range of search keys
	 */
	public abstract void beforeFirst(ConstantRange searchRange);

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
	 */
	public abstract void insert(Constant key, RecordId dataRecordId, boolean doLogicalLogging);

	/**
	 * Deletes the index record having the specified key and data record ID.
	 * 
	 * @param key
	 *            the key of the deleted index record
	 * @param dataRecordId
	 *            the data record ID of the deleted index record
	 */
	public abstract void delete(Constant key, RecordId dataRecordId, boolean doLogicalLogging);

	/**
	 * Closes the index.
	 */
	public abstract void close();

	/**
	 * Preload the index blocks to memory.
	 */
	public abstract void preLoadToMemory();
}
