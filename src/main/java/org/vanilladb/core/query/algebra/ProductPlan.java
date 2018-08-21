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

import java.util.HashSet;
import java.util.Set;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Bucket;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

/**
 * The {@link Plan} class corresponding to the <em>product</em> relational
 * algebra operator.
 */
public class ProductPlan implements Plan {
	/**
	 * Returns a histogram that, for each field, approximates the value
	 * distribution of products from the specified histograms.
	 * 
	 * @param hist1
	 *            the left-hand-side histogram
	 * @param hist2
	 *            the right-hand-side histogram
	 * @return a histogram that, for each field, approximates the value
	 *         distribution of the products
	 */
	public static Histogram productHistogram(Histogram hist1, Histogram hist2) {
		Set<String> prodFlds = new HashSet<String>(hist1.fields());
		prodFlds.addAll(hist2.fields());
		Histogram prodHist = new Histogram(prodFlds);
		double numRec1 = hist1.recordsOutput();
		double numRec2 = hist2.recordsOutput();
		if (Double.compare(numRec1, 1.0) < 0
				|| Double.compare(numRec2, 1.0) < 0)
			return prodHist;
		for (String fld : hist1.fields())
			for (Bucket bkt : hist1.buckets(fld))
				prodHist.addBucket(fld,
						new Bucket(bkt.valueRange(), bkt.frequency() * numRec2,
								bkt.distinctValues(), bkt.valuePercentiles()));
		for (String fld : hist2.fields())
			for (Bucket bkt : hist2.buckets(fld))
				prodHist.addBucket(fld,
						new Bucket(bkt.valueRange(), bkt.frequency() * numRec1,
								bkt.distinctValues(), bkt.valuePercentiles()));
		return prodHist;
	}

	private Plan p1, p2;
	private Schema schema = new Schema();
	private Histogram hist;

	/**
	 * Creates a new product node in the query tree, having the two specified
	 * subqueries.
	 * 
	 * @param p1
	 *            the left-hand subquery
	 * @param p2
	 *            the right-hand subquery
	 */
	public ProductPlan(Plan p1, Plan p2) {
		this.p1 = p1;
		this.p2 = p2;
		schema.addAll(p1.schema());
		schema.addAll(p2.schema());
		hist = productHistogram(p1.histogram(), p2.histogram());
	}

	/**
	 * Creates a product scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan s1 = p1.open();
		Scan s2 = p2.open();
		return new ProductScan(s1, s2);
	}

	/**
	 * Estimates the number of block accesses in the product. The formula is:
	 * 
	 * <pre>
	 * B(product(p1, p2)) = B(p1) + R(p1) * B(p2)
	 * </pre>
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return p1.blocksAccessed() + (p1.recordsOutput() * p2.blocksAccessed());
	}

	/**
	 * Returns the schema of the product, which is the union of the schemas of
	 * the underlying queries.
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

	/**
	 * Returns an estimate of the number of records in the query's output table.
	 * 
	 * @see Plan#recordsOutput()
	 */
	@Override
	public long recordsOutput() {
		return (long) histogram().recordsOutput();
	}

	@Override
	public String toString() {
		String c2 = p2.toString();
		String[] cs2 = c2.split("\n");
		String c1 = p1.toString();
		String[] cs1 = c1.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->ProductPlan  (#blks=" + blocksAccessed() + ", #recs="
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
