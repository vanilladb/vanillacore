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
package org.vanilladb.core.query.algebra.index;

import java.util.Map;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The {@link Plan} class corresponding to the <em>indexselect</em> relational
 * algebra operator.
 */
public class IndexSelectPlan implements Plan {
	private TablePlan tp;
	private IndexInfo ii;
	private Map<String, ConstantRange> searchRanges;
	private Transaction tx;
	private Histogram hist;

	/**
	 * Creates a new index-select node in the query tree for the specified index
	 * and search range.
	 * 
	 * @param tp
	 *            the input table plan
	 * @param ii
	 *            information about the index
	 * @param searchRanges
	 *            the ranges of search keys
	 * @param tx
	 *            the calling transaction
	 */
	public IndexSelectPlan(TablePlan tp, IndexInfo ii,
			Map<String, ConstantRange> searchRanges, Transaction tx) {
		this.tp = tp;
		this.ii = ii;
		this.searchRanges = searchRanges;
		this.tx = tx;
		hist = SelectPlan.constantRangeHistogram(tp.histogram(), searchRanges);
	}

	/**
	 * Creates a new index-select scan for this query
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		// throws an exception if p is not a tableplan.
		TableScan ts = (TableScan) tp.open();
		Index idx = ii.open(tx);
		return new IndexSelectScan(idx, 
				new SearchRange(ii.fieldNames(), schema(), searchRanges), ts);
	}

	/**
	 * Estimates the number of block accesses to compute the index selection,
	 * which is the same as the index traversal cost plus the number of matching
	 * data records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return Index.searchCost(ii.indexType(), new SearchKeyType(schema(), ii.fieldNames()),
				tp.recordsOutput(), recordsOutput()) + recordsOutput();
	}

	/**
	 * Returns the schema of the data table.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return tp.schema();
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return hist;
	}

	@Override
	public long recordsOutput() {
		return (long) histogram().recordsOutput();
	}

	@Override
	public String toString() {
		String c = tp.toString();
		String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->");
		sb.append("IndexSelectPlan cond:" + searchRanges.toString() + " (#blks="
				+ blocksAccessed() + ", #recs=" + recordsOutput() + ")\n");
		for (String child : cs)
			sb.append("\t").append(child).append("\n");
		return sb.toString();
	}
}
