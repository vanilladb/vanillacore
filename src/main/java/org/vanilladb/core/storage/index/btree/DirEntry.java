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
package org.vanilladb.core.storage.index.btree;

import org.vanilladb.core.storage.index.SearchKey;

/**
 * A directory entry has two components: the key of the first record in that
 * block, and the number of the child block.
 */
public class DirEntry {
	private SearchKey key;
	private long blockNum;

	/**
	 * Creates a new entry for the specified key and block number.
	 * 
	 * @param key
	 *            the key
	 * @param blockNum
	 *            the block number
	 */
	public DirEntry(SearchKey key, long blockNum) {
		this.key = key;
		this.blockNum = blockNum;
	}

	/**
	 * Returns the key of the entry
	 * 
	 * @return the key of the entry
	 */
	public SearchKey key() {
		return key;
	}

	/**
	 * Returns the block number component of the entry
	 * 
	 * @return the block number component of the entry
	 */
	public long blockNumber() {
		return blockNum;
	}
}
