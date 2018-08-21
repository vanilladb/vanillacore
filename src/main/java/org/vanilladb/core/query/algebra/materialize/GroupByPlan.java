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
package org.vanilladb.core.query.algebra.materialize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ReduceRecordsPlan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.aggfn.AggregationFn;
import org.vanilladb.core.sql.aggfn.AvgFn;
import org.vanilladb.core.sql.aggfn.CountFn;
import org.vanilladb.core.sql.aggfn.DistinctCountFn;
import org.vanilladb.core.sql.aggfn.MaxFn;
import org.vanilladb.core.sql.aggfn.MinFn;
import org.vanilladb.core.sql.aggfn.SumFn;
import org.vanilladb.core.storage.metadata.statistics.Bucket;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The {@link Plan} class for the <em>groupby</em> operator.
 */
public class GroupByPlan extends ReduceRecordsPlan {
	/**
	 * Returns a histogram that, for each field, approximates distribution of
	 * values in the group-by and aggregation fields.
	 * 
	 * <p>
	 * Assumes that:
	 * <ul>
	 * <li>Distributions of values in group-by fields are independent with each
	 * other</li>
	 * <li>Aggregate values in different groups are distinct</li>
	 * <li></li>
	 * </ul>
	 * 
	 * @param hist
	 *            the input join distribution of field values
	 * @param groupFlds
	 *            the fields to group by. Can be empty, which means that all
	 *            records are in a single group.
	 * @param aggFns
	 *            the aggregation functions. Optional, can be null.
	 * @return a histogram that, for each field, approximates distribution of
	 *         values in the group-by and aggregation fields
	 */
	public static Histogram groupByHistogram(Histogram hist,
			Set<String> groupFlds, Set<AggregationFn> aggFns) {
		if (Double.compare(hist.recordsOutput(), 1.0) < 0)
			return new Histogram(hist.fields());

		double dvProd = 1.0; // the maximal number of group
		for (String fld : groupFlds) {
			double dv = 0.0;
			for (Bucket bkt : hist.buckets(fld))
				dv += bkt.distinctValues();
			dvProd *= dv;
		}
		double numGroups = Math.min(dvProd, hist.recordsOutput());
		double gbReduction = numGroups / hist.recordsOutput();
		Histogram gbHist = new Histogram(groupFlds);
		for (String fld : groupFlds) {
			for (Bucket bkt : hist.buckets(fld)) {
				double newFreq = bkt.frequency() * gbReduction;
				if (Double.compare(newFreq, 1.0) < 0)
					continue;
				gbHist.addBucket(
						fld,
						new Bucket(bkt.valueRange(), newFreq, bkt
								.distinctValues(), bkt.valuePercentiles()));
			}
		}
		if (aggFns != null) {
			for (AggregationFn aggFn : aggFns) {
				String argFld = aggFn.argumentFieldName();
				String fld = aggFn.fieldName();
				Collection<Bucket> dist = hist.buckets(argFld);
				if (dist.isEmpty())
					continue;
				if (aggFn.getClass().equals(SumFn.class))
					gbHist.addBucket(fld,
							sumBucket(dist, numGroups));
				else if (aggFn.getClass().equals(AvgFn.class))
					gbHist.addBucket(fld,
							avgBucket(dist, numGroups));
				else if (aggFn.getClass().equals(CountFn.class))
					gbHist.addBucket(fld,
							countBucket(dist, numGroups));
				else if (aggFn.getClass().equals(DistinctCountFn.class))
					gbHist.addBucket(fld,
							distinctCountBucket(dist, numGroups));
				else if (aggFn.getClass().equals(MinFn.class))
					gbHist.addBucket(fld,
							minBucket(dist, numGroups));
				else if (aggFn.getClass().equals(MaxFn.class))
					gbHist.addBucket(fld,
							maxBucket(dist, numGroups));
				else
					throw new UnsupportedOperationException();
			}
		}
		return syncHistogram(gbHist);
	}

	private static Bucket sumBucket(Collection<Bucket> dist, double numGroups) {
		Constant sumLow = null, sumHigh = new DoubleConstant(1.0);
		double totalFreq = 0.0;
		Map<Constant, Bucket> highs = new HashMap<Constant, Bucket>();
		for (Bucket bkt : dist) {
			// estimate sumLow as the only one smallest value in a group
			if (sumLow == null || bkt.valueRange().low().compareTo(sumLow) < 0)
				sumLow = bkt.valueRange().low();
			totalFreq += bkt.frequency();
			highs.put(bkt.valueRange().high(), bkt);
		}
		SortedSet<Constant> desc = new TreeSet<Constant>(highs.keySet())
				.descendingSet();
		// estimate sumHigh as the sum of top maxGroupSize values
		double maxGroupSize = totalFreq - numGroups + 1;
		double currSize = 0.0;
		for (Constant high : desc) {
			Bucket bkt = highs.get(high);
			double recsToSum = Math.min(bkt.frequency(), maxGroupSize
					- currSize);
			sumHigh = sumHigh.add(high.mul(new DoubleConstant(recsToSum)));
			currSize += recsToSum;
			if (Double.compare(currSize, maxGroupSize) >= 0)
				break;
		}
		ConstantRange sumRange = ConstantRange.newInstance(sumLow, true,
				sumHigh, true);
		// discard percentiles
		return new Bucket(sumRange, numGroups, numGroups);
	}

	private static Bucket avgBucket(Collection<Bucket> dist, double numGroups) {
		Constant avgLow = null, avgHigh = null;
		for (Bucket bkt : dist) {
			if (avgLow == null || bkt.valueRange().low().compareTo(avgLow) < 0)
				avgLow = bkt.valueRange().low();
			if (avgHigh == null
					|| bkt.valueRange().high().compareTo(avgHigh) > 0)
				avgHigh = bkt.valueRange().high();
		}
		ConstantRange avgRange = ConstantRange.newInstance(avgLow, true,
				avgHigh, true);
		// discard percentiles
		return new Bucket(avgRange, numGroups, numGroups);
	}

	private static Bucket countBucket(Collection<Bucket> dist, double numGroups) {
		Constant cntLow = new DoubleConstant(1.0);
		Double totalFreq = 0.0;
		for (Bucket bkt : dist) {
			totalFreq += bkt.frequency();
		}
		double maxGroupSize = totalFreq - numGroups + 1;
		Constant cntHigh = new DoubleConstant(maxGroupSize);
		ConstantRange countRange = ConstantRange.newInstance(cntLow, true,
				cntHigh, true);
		// discard percentiles
		return new Bucket(countRange, numGroups, numGroups);
	}

	private static Bucket distinctCountBucket(Collection<Bucket> dist,
			double numGroups) {
		Constant dcLow = new DoubleConstant(1.0);
		Double totalFreq = 0.0, dv = 0.0;
		for (Bucket bkt : dist) {
			totalFreq += bkt.frequency();
			dv += bkt.distinctValues();
		}
		double maxGroupSize = totalFreq - numGroups + 1;
		Constant dcHigh = new DoubleConstant(Math.min(maxGroupSize, dv));
		ConstantRange distinctCountRange = ConstantRange.newInstance(dcLow,
				true, dcHigh, true);
		// discard percentiles
		return new Bucket(distinctCountRange, numGroups, numGroups);
	}

	private static Bucket minBucket(Collection<Bucket> dist, double numGroups) {
		Constant minLow = null, minHigh = null;
		Double dv = 0.0;
		for (Bucket bkt : dist) {
			if (minLow == null || bkt.valueRange().low().compareTo(minLow) < 0)
				minLow = bkt.valueRange().low();
			if (minHigh == null
					|| bkt.valueRange().high().compareTo(minHigh) > 0)
				minHigh = bkt.valueRange().high();
			dv += bkt.distinctValues();
		}
		ConstantRange minRange = ConstantRange.newInstance(minLow, true,
				minHigh, true);
		// discard percentiles
		return new Bucket(minRange, numGroups, Math.min(numGroups, dv));
	}

	private static Bucket maxBucket(Collection<Bucket> dist, double numGroups) {
		Constant maxLow = null, maxHigh = null;
		Double dv = 0.0;
		for (Bucket bkt : dist) {
			if (maxLow == null || bkt.valueRange().low().compareTo(maxLow) < 0)
				maxLow = bkt.valueRange().low();
			if (maxHigh == null
					|| bkt.valueRange().high().compareTo(maxHigh) > 0)
				maxHigh = bkt.valueRange().high();
			dv += bkt.distinctValues();
		}
		ConstantRange maxRange = ConstantRange.newInstance(maxLow, true,
				maxHigh, true);
		// discard percentiles
		return new Bucket(maxRange, numGroups, Math.min(numGroups, dv));
	}

	private Plan sp;
	private Set<String> groupFlds;
	private Set<AggregationFn> aggFns;
	private Schema schema;
	private Histogram hist;

	/**
	 * Creates a group-by plan for the underlying query. The grouping is
	 * determined by the specified collection of group fields, and the
	 * aggregation is computed by the specified collection of aggregation
	 * functions.
	 * 
	 * @param p
	 *            a plan for the underlying query
	 * @param groupFlds
	 *            the fields to group by. Can be empty, which means that all
	 *            records are in a single group.
	 * @param aggFns
	 *            the aggregation functions. Optional, can be null.
	 * @param tx
	 *            the calling transaction
	 */
	public GroupByPlan(Plan p, Set<String> groupFlds,
			Set<AggregationFn> aggFns, Transaction tx) {
		schema = new Schema();
		this.groupFlds = groupFlds;
		if (!this.groupFlds.isEmpty()) {
			for (String fld : groupFlds)
				schema.add(fld, p.schema());
			// sort records by group-by fields with default direction
			sp = new SortPlan(p, new ArrayList<String>(groupFlds), tx);
		} else
			// all records are in a single group, so p is already sorted
			sp = p;
		this.aggFns = aggFns;
		if (aggFns != null)
			for (AggregationFn fn : aggFns) {
				Type t = fn.isArgumentTypeDependent() ? p.schema().type(
						fn.argumentFieldName()) : fn.fieldType();
				schema.addField(fn.fieldName(), t);
			}
		hist = groupByHistogram(p.histogram(), this.groupFlds, aggFns);
	}

	/**
	 * This method opens a sort plan for the specified plan. The sort plan
	 * ensures that the underlying records will be appropriately grouped.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan ss = sp.open();
		return new GroupByScan(ss, groupFlds, aggFns);
	}

	/**
	 * Returns the number of blocks required to compute the aggregation, which
	 * is one pass through the sorted table. It does <em>not</em> include the
	 * one-time cost of materializing and sorting the records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return sp.blocksAccessed();
	}

	/**
	 * Returns the schema of the output table. The schema consists of the group
	 * fields, plus one field for each aggregation function.
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
		return (long) hist.recordsOutput();
	}

	@Override
	public String toString() {
		String c = sp.toString();
		String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->");
		sb.append("GroupByPlan: (#blks=" + blocksAccessed() + ", #recs="
				+ recordsOutput() + ")\n");
		for (String child : cs)
			sb.append("\t").append(child).append("\n");
		;
		return sb.toString();
	}
}
