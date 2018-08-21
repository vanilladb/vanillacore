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
package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

/**
 * The interface implemented by each query plan. There is a Plan class for each
 * relational algebra operator.
 */
public interface Plan {

	/**
	 * Opens a scan corresponding to this plan. The scan will be positioned
	 * before its first record.
	 * 
	 * @return a scan
	 */
	Scan open();

	/**
	 * Returns an estimate of the number of block accesses that will occur when
	 * the scan is read to completion.
	 * 
	 * @return the estimated number of block accesses
	 */
	long blocksAccessed();

	/**
	 * Returns the schema of the query.
	 * 
	 * @return the query's schema
	 */
	Schema schema();

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @return the histogram
	 */
	Histogram histogram();

	/**
	 * Returns an estimate of the number of records in the query's output table.
	 * 
	 * @return the estimated number of output records
	 */
	long recordsOutput();
}
