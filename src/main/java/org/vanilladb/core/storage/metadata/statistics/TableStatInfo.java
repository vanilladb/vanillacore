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
package org.vanilladb.core.storage.metadata.statistics;

/**
 * Holds statistical information about a table.
 */
public class TableStatInfo {
	private long numBlks;
	private Histogram hist;

	TableStatInfo(long numBlks, Histogram hist) {
		this.numBlks = numBlks;
		this.hist = hist;
	}

	/**
	 * Returns the estimated number of blocks in the table.
	 * 
	 * @return the estimated number of blocks in the table
	 */
	public long blocksAccessed() {
		return numBlks;
	}

	/**
	 * Returns a histogram that approximates the join distribution of
	 * frequencies of field values in this table.
	 * 
	 * @return the histogram
	 */
	public Histogram histogram() {
		return hist;
	}
}
