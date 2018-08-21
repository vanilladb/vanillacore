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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.storage.metadata.statistics.Bucket;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.metadata.statistics.Percentiles;

/**
 * The {@link Plan} class corresponding to the <em>select</em> relational
 * algebra operator.
 */
public class SelectPlan extends ReduceRecordsPlan {
	/**
	 * Returns a histogram that, for each field, approximates the distribution
	 * of field values from the specified histogram satisfying the specified
	 * predicate.
	 * 
	 * <p>
	 * Assumes that:
	 * <ul>
	 * <li>Equality selection always finds matching records</li>
	 * <li>Values in a bucket have the same frequency (uniform frequency)</li>
	 * <li>Given values within two equal ranges (of two joinable fields), all
	 * values in the range having smaller number of values appear in the range
	 * having larger number of values</li>
	 * <li>Distributions of values in different fields are independent with each
	 * other</li>
	 * </ul>
	 * 
	 * @param hist
	 *            the input join distribution of field values
	 * @param pred
	 *            the predicate
	 * @return a histogram that, for each field, approximates the distribution
	 *         of field values satisfying the predicate
	 */
	public static Histogram predHistogram(Histogram hist, Predicate pred) {
		if (Double.compare(hist.recordsOutput(), 1.0) < 0)
			return new Histogram(hist.fields());

		// apply constant ranges
		Map<String, ConstantRange> cRanges = new HashMap<String, ConstantRange>();
		for (String fld : hist.fields()) {
			ConstantRange cr = pred.constantRange(fld);
			if (cr != null)
				cRanges.put(fld, cr);
		}
		Histogram crHist = constantRangeHistogram(hist, cRanges);

		// apply field joins
		Histogram jfHist = crHist;
		Deque<String> flds = new LinkedList<String>(jfHist.fields());
		while (!flds.isEmpty()) {
			String fld = flds.removeFirst();
			Set<String> group = pred.joinFields(fld);
			if (group != null) {
				flds.removeAll(group);
				group.add(fld);
				jfHist = joinFieldsHistogram(jfHist, group);
			}
		}
		return jfHist;
	}

	/**
	 * Returns a histogram that, for each field, approximates the distribution
	 * of values from the specified histogram falling within the specified
	 * search range.
	 * 
	 * <p>
	 * Assumes that:
	 * <ul>
	 * <li>Equality selection always finds matching records</li>
	 * <li>Values in a bucket have the same frequency (uniform frequency)</li>
	 * <li>Distributions of values in different fields are independent with each
	 * other</li>
	 * </ul>
	 * 
	 * @param hist
	 *            the input histogram
	 * @param cRanges
	 *            a map from fields to search ranges
	 * @return a histogram that, for each field, approximates the distribution
	 *         of values from the specified histogram falling within the
	 *         specified search range
	 */
	public static Histogram constantRangeHistogram(Histogram hist,
			Map<String, ConstantRange> cRanges) {
		if (Double.compare(hist.recordsOutput(), 1.0) < 0)
			return new Histogram(hist.fields());

		Histogram crHist = new Histogram(hist);
		for (String fld : cRanges.keySet()) {
			Collection<Bucket> crBkts = new ArrayList<Bucket>(crHist.buckets(
					fld).size());
			ConstantRange cr = cRanges.get(fld);
			double freqSum = 0.0;
			for (Bucket bkt : crHist.buckets(fld)) {
				Bucket crBkt = constantRangeBucket(bkt, cr);
				if (crBkt != null) {
					crBkts.add(crBkt);
					freqSum += crBkt.frequency();
				}
			}
			if (Double.compare(freqSum, 1.0) < 0) // no bucket in range
				return new Histogram(hist.fields());
			double crReduction = freqSum / crHist.recordsOutput();
			if (Double.compare(crReduction, 1.0) == 0)
				continue;
			// update this field's buckets
			crHist.setBuckets(fld, crBkts);
			/*
			 * update other fields' buckets to ensure that all fields have the
			 * same total frequencies.
			 */
			for (String restFld : crHist.fields()) {
				if (restFld.equals(fld))
					continue;
				Collection<Bucket> restBkts = new ArrayList<Bucket>(crHist
						.buckets(restFld).size());
				for (Bucket bkt : crHist.buckets(restFld)) {
					double restFreq = bkt.frequency() * crReduction;
					if (Double.compare(restFreq, 1.0) < 0)
						continue;
					double restDistVals = Math.min(bkt.distinctValues(),
							restFreq);
					Bucket restBkt = new Bucket(bkt.valueRange(), restFreq,
							restDistVals, bkt.valuePercentiles());
					restBkts.add(restBkt);
				}
				crHist.setBuckets(restFld, restBkts);
			}
		}
		return syncHistogram(crHist);
	}

	/**
	 * Creates a new bucket by keeping the statistics of records and values in
	 * the specified bucket falling within the specified search range.
	 * 
	 * <p>
	 * Assumes that:
	 * <ul>
	 * <li>Equality selection always finds matching records</li>
	 * <li>Values in a bucket have the same frequency (uniform frequency)</li>
	 * </ul>
	 * 
	 * @param bkt
	 *            the input bucket
	 * @param cRange
	 *            the search range
	 * @return a new bucket that keeps the statistics of records and values
	 *         falling within the specified search range
	 */
	public static Bucket constantRangeBucket(Bucket bkt, ConstantRange cRange) {
		ConstantRange newRange = bkt.valueRange().intersect(cRange);
		if (!newRange.isValid())
			return null;
		double newDistVals = bkt.distinctValues(newRange);
		if (Double.compare(newDistVals, 1.0) < 0)
			return null;
		double newFreq = bkt.frequency() * newDistVals / bkt.distinctValues();
		if (bkt.valuePercentiles() == null)
			return new Bucket(newRange, newFreq, newDistVals);
		Percentiles newPcts = bkt.valuePercentiles().percentiles(newRange);
		return new Bucket(newRange, newFreq, newDistVals, newPcts);
	}

	/**
	 * Returns a histogram that, for each field, approximates the distribution
	 * of values from the specified histogram joining with other fields in the
	 * specified group.
	 * 
	 * <p>
	 * Assumes that:
	 * <ul>
	 * <li>Values in a bucket have the same frequency (uniform frequency)</li>
	 * <li>Given values within two equal ranges (of two joinable fields), all
	 * values in the range having smaller number of values appear in the range
	 * having larger number of values</li>
	 * <li>Distributions of values in different fields are independent with each
	 * other</li>
	 * </ul>
	 * 
	 * @param hist
	 *            the input histogram
	 * @param group
	 *            the group of joining fields
	 * @return a histogram that, for each field, approximates the distribution
	 *         of values from the specified histogram joining with other fields
	 *         in the specified group
	 */
	public static Histogram joinFieldsHistogram(Histogram hist,
			Set<String> group) {
		if (group.size() < 2)
			return new Histogram(hist);
		List<String> flds = new ArrayList<String>(group);
		Collection<Bucket> jfBkts = hist.buckets(flds.get(0));
		for (int i = 1; i < flds.size(); i++) {
			Collection<Bucket> temp = jfBkts;
			jfBkts = new ArrayList<Bucket>(2 * jfBkts.size());
			for (Bucket bkt1 : temp) {
				for (Bucket bkt2 : hist.buckets(flds.get(i))) {
					Bucket jfBkt = joinFieldBucket(bkt1, bkt2,
							hist.recordsOutput());
					if (jfBkt != null)
						jfBkts.add(jfBkt);
				}
			}
		}
		double freqSum = 0.0;
		for (Bucket bkt : jfBkts)
			freqSum += bkt.frequency();
		if (Double.compare(freqSum, 1.0) < 0) // no joined bucket
			return new Histogram(hist.fields());
		double jfReduction = freqSum / hist.recordsOutput();
		if (Double.compare(jfReduction, 1.0) == 0)
			return new Histogram(hist);
		Histogram jfHist = new Histogram(hist.fields());
		for (String fld : hist.fields()) {
			if (group.contains(fld))
				jfHist.setBuckets(fld, jfBkts);
			else {
				for (Bucket bkt : hist.buckets(fld)) {
					double restFreq = bkt.frequency() * jfReduction;
					if (Double.compare(restFreq, 1.0) < 0)
						continue;
					double restDistVals = Math.min(bkt.distinctValues(),
							restFreq);
					Bucket restBkt = new Bucket(bkt.valueRange(), restFreq,
							restDistVals, bkt.valuePercentiles());
					jfHist.addBucket(fld, restBkt);
				}
			}
		}
		return syncHistogram(jfHist);
	}

	/**
	 * Creates a new bucket by keeping the statistics of joining records and
	 * values from the two specified buckets.
	 * 
	 * <p>
	 * Assumes that:
	 * <ul>
	 * <li>Values in a bucket have the same frequency (uniform frequency)</li>
	 * <li>Given values within two equal ranges (of two joinable fields), all
	 * values in the range having smaller number of values appear in the range
	 * having larger number of values</li>
	 * <li>Distributions of values in different fields are independent with each
	 * other</li>
	 * </ul>
	 * 
	 * @param bkt1
	 *            the input bucket 1
	 * @param bkt2
	 *            the input bucket 2
	 * @param numRec
	 *            the total number of records in the histogram where
	 *            <code>bkt1</code> and <code>bkt2</code> belong to
	 * @return a new bucket that keeps the statistics of joining records and
	 *         values from the two specified buckets
	 */
	public static Bucket joinFieldBucket(Bucket bkt1, Bucket bkt2, double numRec) {
		ConstantRange newRange = bkt1.valueRange().intersect(bkt2.valueRange());
		if (!newRange.isValid())
			return null;
		double rdv1 = bkt1.distinctValues(newRange);
		double rdv2 = bkt2.distinctValues(newRange);
		double newDistVals = Math.min(rdv1, rdv2);
		if (Double.compare(newDistVals, 1.0) < 0)
			return null;
		double newFreq = Math.min(
				bkt1.frequency() * (bkt2.frequency() / numRec)
						* (newDistVals / bkt1.distinctValues()) / rdv2,
				bkt2.frequency() * (bkt1.frequency() / numRec)
						* (newDistVals / bkt2.distinctValues()) / rdv1);
		if (Double.compare(newFreq, 1.0) < 0)
			return null;
		Bucket smaller = rdv1 < rdv2 ? bkt1 : bkt2;
		if (smaller.valuePercentiles() == null)
			return new Bucket(newRange, newFreq, newDistVals);
		Percentiles newPcts = smaller.valuePercentiles().percentiles(newRange);
		return new Bucket(newRange, newFreq, newDistVals, newPcts);
	}

	private Plan p;
	private Predicate pred;
	private Histogram hist;

	/**
	 * Creates a new select node in the query tree, having the specified
	 * subquery and predicate.
	 * 
	 * @param p
	 *            the subquery
	 * @param pred
	 *            the predicate
	 */
	public SelectPlan(Plan p, Predicate pred) {
		this.p = p;
		this.pred = pred;
		hist = predHistogram(p.histogram(), pred);
	}

	/**
	 * Creates a select scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan s = p.open();
		return new SelectScan(s, pred);
	}

	/**
	 * Estimates the number of block accesses in the selection.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Returns the schema of the selection, which is the same as in the
	 * underlying query.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return p.schema();
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
		sb.append("->SelectPlan pred:(" + pred.toString() + ") (#blks="
				+ blocksAccessed() + ", #recs=" + recordsOutput() + ")\n");
		for (String child : cs)
			sb.append("\t").append(child).append("\n");
		return sb.toString();
	}
}
