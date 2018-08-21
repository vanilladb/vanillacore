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
package org.vanilladb.core.storage.record;

import org.vanilladb.core.storage.file.BlockId;

/**
 * An identifier for a record within a file. A RecordId consists of the block
 * number in the file, and the ID of the record in that block.
 */
public class RecordId implements Comparable<RecordId> {
	private BlockId blk;
	private int id;

	/**
	 * Creates a record ID for the record having the specified ID in the
	 * specified block.
	 * 
	 * @param blk
	 *            the block id where the record lives
	 * @param id
	 *            the record's ID
	 */
	public RecordId(BlockId blk, int id) {
		this.blk = blk;
		this.id = id;
	}

	/**
	 * Returns the block associated with this record ID.
	 * 
	 * @return the block
	 */
	public BlockId block() {
		return blk;
	}

	/**
	 * Returns the ID associated with this record ID.
	 * 
	 * @return the ID
	 */
	public int id() {
		return id;
	}
	
	@Override
	public int compareTo(RecordId rid) {
		int blkResult = blk.compareTo(rid.blk);
		if (blkResult != 0)
			return blkResult;
		
		if (id < rid.id)
			return -1;
		else if (id > rid.id)
			return 1;
		
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj.getClass().equals(RecordId.class)))
			return false;
		RecordId r = (RecordId) obj;
		return blk.equals(r.blk) && id == r.id;
	}

	public String toString() {
		return "[file " + blk.fileName() + ", block " + blk.number()
				+ ", record " + id + "]";
	}

	@Override
	public int hashCode() {
		int hash = 17;
		hash = hash * 31 + blk.hashCode();
		hash = hash * 31 + id;
		return hash;
	}
}
