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

import static org.vanilladb.core.sql.Type.DOUBLE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.util.CoreProperties;

/**
 * Performs the slide-window uniform sampling on records and by using the
 * samples, and constructs histograms with the "MaxDiff(V, A)" buckets for
 * numeric field values and "MaxDiff(V, F)" buckets for other types of field
 * values respectively.
 */
public class SampledHistogramBuilder {
	private static final int MAX_SAMPLES;

	static {
		MAX_SAMPLES = CoreProperties.getLoader().getPropertyAsInteger(
				SampledHistogramBuilder.class.getName() + ".MAX_SAMPLES", 1000);
	}

	private static abstract class BucketBuilder {
		Map<Constant, Integer> freqs;

		BucketBuilder(Map<Constant, Integer> freqs) {
			this.freqs = freqs;
		}

		abstract Cut maxCut();

		abstract BucketBuilder split(Cut cut);

		/**
		 * Creates a {@link Bucket} instance. The frequency and number of
		 * distinct values are extrapolated.
		 * 
		 * @param totalFreq
		 *            the total number of records in the table
		 * @param freqExtra
		 *            the factor of extrapolation (no less than 1) for the
		 *            number of records
		 * @param valExtra
		 *            the factor of extrapolation (no less than 1) for the
		 *            number of distinct values
		 * @return
		 */
		abstract Bucket asBucket(double freqExtra, double valExtra);

		Map<Constant, Integer> splitFreqs(Cut cut) {
			Map<Constant, Integer> newFreqs = new HashMap<Constant, Integer>();
			Iterator<Constant> si = freqs.keySet().iterator();
			while (si.hasNext()) {
				Constant v = si.next();
				if (v.compareTo(cut.value()) < 0) {
					newFreqs.put(v, freqs.get(v));
					si.remove();
				}
			}
			return newFreqs;
		}

		ConstantRange valRange() {
			Constant l = null, h = null;
			for (Constant v : freqs.keySet()) {
				if (l == null || v.compareTo(l) < 0)
					l = v;
				if (h == null || v.compareTo(h) > 0)
					h = v;
			}
			// return null if no record in this bucket
			if (l == null && h == null)
				return null;
			return ConstantRange.newInstance(l, true, h, true);
		}

		/**
		 * Returns the total frequency of values in this instance.
		 * 
		 * @return total frequency of values in this instance
		 */
		int totalFreq() {
			int sum = 0;
			for (Integer i : freqs.values())
				sum += i;
			return sum;
		}
	}

	/**
	 * Constructs histograms with the "MaxDiff(V, A)". MaxDiff(V, A) is used to
	 * find a cut that maximize the difference between frequencies and the
	 * difference between spreads.
	 * 
	 */
	private static class MaxDiffAreaBucketBuilder extends BucketBuilder {
		Cut cut;
		SortedSet<Constant> sortedDvs;
		int totalFreq;
		double rangeLength;

		MaxDiffAreaBucketBuilder(Map<Constant, Integer> freqs, Cut cut) {
			super(freqs);
			this.cut = cut;
			sortedDvs = new TreeSet<Constant>(freqs.keySet());
			totalFreq = totalFreq();
			if (totalFreq == 0)
				rangeLength = 0;
			else
				rangeLength = (Double) sortedDvs.last().sub(sortedDvs.first())
						.castTo(DOUBLE).asJavaVal();
		}

		@Override
		Cut maxCut() {
			Cut maxCut = null;
			Iterator<Constant> vi = sortedDvs.iterator();
			Constant prev = null, curr = null, next = null;
			while (vi.hasNext()) {

				prev = curr;
				curr = next;
				next = vi.next();
				if (prev != null && curr != null) {
					double prevArea = normArea(prev, curr);
					double currArea = normArea(curr, next);
					double diff = Math.abs(currArea - prevArea);
					if (maxCut == null || diff > maxCut.diff()) {
						/*
						 * If max cut is resulted by the difference between
						 * frequencies, put curr to the right of the cut;
						 * otherwise if it is resulted by the difference between
						 * spreads, put curr to the side closer.
						 */
						double freqDiff = Math.abs(normFreq(curr)
								- normFreq(prev));
						double spreadDiff = Math.abs(normSpread(curr, next)
								- normSpread(prev, curr));
						if (freqDiff > spreadDiff
								|| normSpread(prev, curr) > normSpread(curr,
										next))
							maxCut = new Cut(curr, diff);
						else
							maxCut = new Cut(next, diff);
					}
				}
			}
			// take the diff between the last two values into account
			if (curr != null && next != null) {
				prev = curr;
				curr = next;
				Constant lastSpread = curr.sub(prev);
				next = curr.add(lastSpread);
				if (cut != null && cut.value().compareTo(next) < 0)
					next = cut.value();
				double prevArea = normArea(prev, curr);
				double currArea = normArea(curr, next);
				double diff = Math.abs(currArea - prevArea);
				if (maxCut == null || diff > maxCut.diff())
					maxCut = new Cut(curr, diff);
			}
			return maxCut;
		}

		double normArea(Constant l, Constant h) {
			return normFreq(l) * normSpread(l, h);
		}

		double normFreq(Constant val) {
			return freqs.get(val) / (double) totalFreq;
		}

		double normSpread(Constant l, Constant h) {
			return (Double) h.sub(l).castTo(DOUBLE).asJavaVal() / rangeLength;
		}

		@Override
		BucketBuilder split(Cut cut) {
			Map<Constant, Integer> newFreqs = splitFreqs(cut);
			// re-init
			sortedDvs = new TreeSet<Constant>(freqs.keySet());
			totalFreq = totalFreq();
			rangeLength = (Double) sortedDvs.last().sub(sortedDvs.first())
					.castTo(DOUBLE).asJavaVal();
			return new MaxDiffAreaBucketBuilder(newFreqs, cut);
		}

		@Override
		Bucket asBucket(double freqExtra, double valExtra) {
			return new Bucket(valRange(), totalFreq() * freqExtra, freqs
					.keySet().size() * valExtra);
		}
	}

	private static class MaxDiffFreqBucketBuilder extends BucketBuilder {
		int numPcts;

		MaxDiffFreqBucketBuilder(Map<Constant, Integer> freqs, int numPcts) {
			super(freqs);
			this.numPcts = numPcts;
		}

		@Override
		Cut maxCut() {
			Cut maxCut = null;
			SortedSet<Constant> sorted = new TreeSet<Constant>(freqs.keySet());
			Iterator<Constant> vi = sorted.iterator();
			Constant prev = null, curr = null;
			while (vi.hasNext()) {
				prev = curr;
				curr = vi.next();
				if (prev != null && curr != null) {
					double diff = Math.abs(freqs.get(curr) - freqs.get(prev));
					if (maxCut == null || diff > maxCut.diff())
						maxCut = new Cut(curr, diff);
				}
			}
			return maxCut;
		}

		@Override
		BucketBuilder split(Cut cut) {
			Map<Constant, Integer> newFreqs = splitFreqs(cut);
			return new MaxDiffFreqBucketBuilder(newFreqs, numPcts);
		}

		@Override
		Bucket asBucket(double freqExtra, double valExtra) {
			if (numPcts < 1)
				new Bucket(valRange(), totalFreq() * freqExtra, freqs.keySet()
						.size() * valExtra);

			Map<Constant, Double> pcts = new HashMap<Constant, Double>();
			SortedSet<Constant> sorted = new TreeSet<Constant>(freqs.keySet());
			double pIdx = 1.0 / numPcts;
			int i = 0, p = 1;
			for (Constant v : sorted) {
				i++;
				while (Double.compare((double) i / sorted.size(), pIdx) >= 0) {
					p++;
					pcts.put(v, pIdx);
					pIdx = (p * 1.0) / numPcts;
				}
			}
			return new Bucket(valRange(), totalFreq() * freqExtra, freqs
					.keySet().size() * valExtra, new Percentiles(pcts));
		}
	}

	private static class Cut {
		Constant value;
		double diff;

		/**
		 * Represents a cut at given value on a range. For example, cut with
		 * value 5 for range [1, 10] will split the range into [1, 5) and [5,
		 * 10].
		 * 
		 */
		Cut(Constant value, double diff) {
			this.value = value;
			this.diff = diff;
		}

		Constant value() {
			return value;
		}

		double diff() {
			return diff;
		}
	}

	private static class Sample implements Record {
		Map<String, Constant> fldVals;

		Sample(Record rec, Schema schema) {
			fldVals = new HashMap<String, Constant>();
			for (String fld : schema.fields()) {
				fldVals.put(fld, rec.getVal(fld));
			}
		}

		@Override
		public Constant getVal(String fldName) {
			return fldVals.get(fldName);
		}

	}

	private Schema schema;
	private List<Record> samples;
	private long totalRecs;
	private Random random;

	// members used to extrapolate the number of distinct values
	private Map<String, Set<Constant>> dvs; // distinct values of flds
	private Map<String, Integer> rpvs; // record per value
	private Map<String, Integer> maxRpvs;

	public SampledHistogramBuilder(Schema schema) {
		this.schema = schema;
		samples = new LinkedList<Record>();
		random = new Random();

		dvs = new HashMap<String, Set<Constant>>();
		rpvs = new HashMap<String, Integer>();
		maxRpvs = new HashMap<String, Integer>();
		for (String fld : schema.fields()) {
			dvs.put(fld, new HashSet<Constant>());
			rpvs.put(fld, 0);
			maxRpvs.put(fld, 0);
		}
	}

	/**
	 * Keep a record as a sample, with certain probability. This method is
	 * designed to uniformly sample all records of a table under the situation
	 * where the total number of records is unknown in advance. A client should
	 * call this method when iterating through each record of a table.
	 * 
	 * @param rec
	 *            the record
	 */
	public void sample(Record rec) {
		totalRecs++;
		if (samples.size() < MAX_SAMPLES) {
			samples.add(new Sample(rec, schema));
			updateNewValueInterval(rec);
		} else {
			double flip = random.nextDouble();
			if (flip < (double) MAX_SAMPLES / totalRecs) {
				samples.set(random.nextInt(MAX_SAMPLES),
						new Sample(rec, schema));
				updateNewValueInterval(rec);
			}
		}
	}

	private void updateNewValueInterval(Record sample) {
		// update distinct value and records per value
		for (String fld : schema.fields()) {
			rpvs.put(fld, rpvs.get(fld) + 1);
			if (!dvs.get(fld).contains(sample.getVal(fld))) {
				dvs.get(fld).add(sample.getVal(fld));
				if (rpvs.get(fld) > maxRpvs.get(fld))
					maxRpvs.put(fld, rpvs.get(fld));
				rpvs.put(fld, 0);
			}
		}
	}

	/**
	 * Constructs a histogram with the "MaxDiff(V, A)" buckets for numeric field
	 * values and "MaxDiff(V, F)" buckets for other types of field values
	 * respectively.
	 * 
	 * @param numBkts
	 *            the number of buckets to construct for each field
	 * @param numPcts
	 *            the number of value percentiles in each bucket of the
	 *            non-numeric fields
	 * @return a "MaxDiff(V, A/F)" histogram
	 */
	public Histogram newMaxDiffHistogram(int numBkts, int numPcts) {
		Map<String, BucketBuilder> initBbs = new HashMap<String, BucketBuilder>();
		for (String fld : schema.fields()) {
			if (schema.type(fld).isNumeric()) {
				initBbs.put(fld, new MaxDiffAreaBucketBuilder(frequencies(fld),
						null));
			} else
				initBbs.put(fld, new MaxDiffFreqBucketBuilder(frequencies(fld),
						numPcts));
		}
		return newMaxDiffHistogram(numBkts, initBbs);
	}

	/**
	 * Constructs a histogram with the "MaxDiff(V, A)" buckets for all fields.
	 * All fields must be numeric.
	 * 
	 * @param numBkts
	 *            the number of buckets to construct for each field
	 * @return a "MaxDiff(V, A)" histogram
	 */
	public Histogram newMaxDiffAreaHistogram(int numBkts) {
		Map<String, BucketBuilder> initBbs = new HashMap<String, BucketBuilder>();
		for (String fld : schema.fields()) {
			initBbs.put(fld, new MaxDiffAreaBucketBuilder(frequencies(fld),
					null));
		}
		return newMaxDiffHistogram(numBkts, initBbs);
	}

	/**
	 * Constructs a histogram with the "MaxDiff(V, F)" buckets for all fields.
	 * 
	 * @param numBkts
	 *            the number of buckets to construct for each field
	 * @param numPcts
	 *            the number of value percentiles in each bucket
	 * @return a "MaxDiff(V, F)" histogram
	 */
	public Histogram newMaxDiffFreqHistogram(int numBkts, int numPcts) {
		Map<String, BucketBuilder> initBbs = new HashMap<String, BucketBuilder>();
		// initialize the first bucket builder for every field
		for (String fld : schema.fields())
			initBbs.put(fld, new MaxDiffFreqBucketBuilder(frequencies(fld),
					numPcts));
		return newMaxDiffHistogram(numBkts, initBbs);
	}

	private Histogram newMaxDiffHistogram(int numBkts,
			Map<String, BucketBuilder> initBbs) {
		Map<String, Collection<Bucket>> dists = new HashMap<String, Collection<Bucket>>();
		for (String fld : schema.fields()) {
			List<BucketBuilder> bbs = new LinkedList<BucketBuilder>();
			bbs.add(initBbs.get(fld));

			// splits the bb having the max diff until there are enough bbs
			while (bbs.size() < numBkts) {
				Cut maxCut = null;
				BucketBuilder maxBb = null;
				// finds the max cut among all existing buckets
				for (BucketBuilder bb : bbs) {
					Cut cut = bb.maxCut();
					if (maxCut == null
							|| (cut != null && cut.diff() > maxCut.diff())) {
						maxCut = cut;
						maxBb = bb;
					}
				}
				if (maxCut == null)
					break; // returns early if each bb contains only one value
				BucketBuilder newBb = maxBb.split(maxCut);
				bbs.add(newBb);
			}

			Collection<Bucket> dist = new ArrayList<Bucket>(bbs.size());
			for (BucketBuilder bb : bbs) {
				Bucket bkt = bb.asBucket(extrapolateRecords(),
						extrapolateValues(fld));
				dist.add(bkt);
			}
			dists.put(fld, dist);
		}
		return new Histogram(dists);
	}

	private Map<Constant, Integer> frequencies(String fldName) {
		Map<Constant, Integer> freqs = new HashMap<Constant, Integer>();
		for (Record rec : samples) {
			Constant v = rec.getVal(fldName);
			Integer f = freqs.get(v);
			freqs.put(v, f == null ? 1 : f + 1);
		}
		return freqs;
	}

	private double extrapolateRecords() {
		return totalRecs <= MAX_SAMPLES ? 1.0 : (double) totalRecs
				/ MAX_SAMPLES;
	}

	private double extrapolateValues(String fldName) {
		int dv = dvs.get(fldName).size();
		// if this file is empty when being sampled
		if (dv == 0)
			return 1;
		int rpv = Math.max(rpvs.get(fldName) * 2, maxRpvs.get(fldName));
		return (((double) totalRecs - samples.size()) / rpv + dv) / dv;
	}
}
