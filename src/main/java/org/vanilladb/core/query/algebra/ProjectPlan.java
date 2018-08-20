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

import java.util.Set;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

/**
 * The {@link Plan} class corresponding to the <em>project</em> relational
 * algebra operator.
 */
public class ProjectPlan implements Plan {
	/**
	 * Returns a histogram that approximates the join frequency distribution of
	 * the projected values from the specified histograms onto the specified
	 * fields.
	 * 
	 * @param hist
	 *            the input join distribution of field values
	 * @param fldNames
	 *            the names of fields to project to
	 * @return join distribution of projected values
	 */
	public static Histogram projectHistogram(Histogram hist,
			Set<String> fldNames) {
		Histogram pjtHist = new Histogram(fldNames);
		for (String fld : fldNames)
			pjtHist.setBuckets(fld, hist.buckets(fld));
		return pjtHist;
	}

	private Plan p;
	private Schema schema = new Schema();
	private Histogram hist;

	/**
	 * Creates a new project node in the query tree, having the specified
	 * subquery and field list.
	 * 
	 * @param p
	 *            the subquery
	 * @param fldNames
	 *            the list of fields
	 */
	public ProjectPlan(Plan p, Set<String> fldNames) {
		this.p = p;
		for (String fldname : fldNames)
			schema.add(fldname, p.schema());
		hist = projectHistogram(p.histogram(), fldNames);
	}

	/**
	 * Creates a project scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan s = p.open();
		return new ProjectScan(s, schema.fields());
	}

	/**
	 * Estimates the number of block accesses in the projection, which is the
	 * same as in the underlying query.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Returns the schema of the projection, which is taken from the field list.
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
		String c = p.toString();
		String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->ProjectPlan  (#blks=" + blocksAccessed() + ", #recs="
				+ recordsOutput() + ")\n");
		for (String child : cs)
			sb.append("\t").append(child).append("\n");
		return sb.toString();
	}
}
