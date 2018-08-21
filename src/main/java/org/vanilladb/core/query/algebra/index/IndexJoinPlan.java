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
package org.vanilladb.core.query.algebra.index;

import java.util.Map;

import org.vanilladb.core.query.algebra.AbstractJoinPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The {@link Plan} class corresponding to the <em>indexjoin</em> relational
 * algebra operator.
 */
public class IndexJoinPlan extends AbstractJoinPlan {
	private Plan p1;
	private TablePlan tp2;
	private IndexInfo ii;
	private Map<String, String> joinFields; // <LHS field -> RHS field>
	private Schema schema = new Schema();
	private Transaction tx;
	private Histogram hist;

	/**
	 * Implements the join operator, using the specified LHS and RHS plans.
	 * 
	 * @param p1
	 *            the left-hand plan
	 * @param tp2
	 *            the right-hand table plan
	 * @param ii
	 *            information about the right-hand index
	 * @param joinFields
	 *            the mapping of join fields from LHS to RHS
	 * @param tx
	 *            the calling transaction
	 */
	public IndexJoinPlan(Plan p1, TablePlan tp2, IndexInfo ii,
			Map<String, String> joinFields, Transaction tx) {
		this.p1 = p1;
		this.tp2 = tp2;
		this.ii = ii;
		this.joinFields = joinFields;
		this.tx = tx;
		schema.addAll(p1.schema());
		schema.addAll(tp2.schema());
		
		// XXX: It needs to be updated for multi-key indexes
		for (String lhsField : joinFields.keySet()) {
			hist = joinHistogram(p1.histogram(), tp2.histogram(), lhsField,
					joinFields.get(lhsField));
			break;
		}
	}

	/**
	 * Opens an index-join scan for this query
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan s = p1.open();
		// throws an exception if p2 is not a tableplan
		TableScan ts = (TableScan) tp2.open();
		Index idx = ii.open(tx);
		return new IndexJoinScan(s, idx, joinFields, ts);
	}

	/**
	 * Estimates the number of block accesses to compute the join. The formula
	 * is:
	 * 
	 * <pre>
	 * B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx)
	 *       + R(indexjoin(p1,p2,idx)
	 * </pre>
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		// block accesses to search for a join record in the index
		long searchCost = Index.searchCost(ii.indexType(),
				new SearchKeyType(schema(), ii.fieldNames()), tp2.recordsOutput(), 1);
		return p1.blocksAccessed() + (p1.recordsOutput() * searchCost)
				+ recordsOutput();
	}

	/**
	 * Returns the schema of the index join.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return schema;
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
		String c2 = tp2.toString();
		String[] cs2 = c2.split("\n");
		String c1 = p1.toString();
		String[] cs1 = c1.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->");
		sb.append("IndexJoinPlan (#blks=" + blocksAccessed() + ", #recs="
				+ recordsOutput() + ")\n");
		// right child
		for (String child : cs2)
			sb.append("\t").append(child).append("\n");
		// left child
		for (String child : cs1)
			sb.append("\t").append(child).append("\n");
		return sb.toString();
	}
}
