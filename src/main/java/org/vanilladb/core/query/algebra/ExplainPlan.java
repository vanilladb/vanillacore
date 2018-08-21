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
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

/**
 * The {@link Plan} class corresponding to the <em>explain</em> relational
 * algebra operator.
 */
public class ExplainPlan implements Plan {
	private Plan p;

	/**
	 * Creates a new explain node in the query tree, having the specified query.
	 * 
	 * @param p
	 *            the underlying query plan
	 */
	public ExplainPlan(Plan p) {
		this.p = p;
	}

	/**
	 * Creates a explain scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		return new ExplainScan(p.open(), schema(), p.toString());
	}

	/**
	 * Estimates the number of block accesses for answering explain query.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Returns the schema of the explain query, which has only one field
	 * "query-plan" of type varchar(500).
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		Schema schema = new Schema();
		schema.addField("query-plan", Type.VARCHAR(500));
		return schema;
	}

	/**
	 * Returns the histogram that approximates the distribution of the
	 * underlying query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		return 1;
	}
}
