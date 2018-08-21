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
package org.vanilladb.core.storage.metadata.statistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A histogram that, for each field of a table, approximates the distribution of
 * values using {@link Bucket buckets}.
 * <p>
 * Assumes that, for each field:
 * <ul>
 * <li>Sets of records in different buckets are disjoint.</li>
 * <li>Sets of values in different buckets are disjoint.</li>
 * </ul>
 */
public class Histogram {
	private Map<String, Collection<Bucket>> dists;

	public Histogram() {
		dists = new HashMap<String, Collection<Bucket>>();
	}

	public Histogram(Set<String> fldNames) {
		dists = new HashMap<String, Collection<Bucket>>();
		for (String fld : fldNames)
			dists.put(fld, new ArrayList<Bucket>());
	}

	Histogram(Map<String, Collection<Bucket>> dists) {
		this.dists = dists;
	}

	public Histogram(Histogram hist) {
		dists = new HashMap<String, Collection<Bucket>>();
		for (String fld : hist.dists.keySet()) {
			Collection<Bucket> bkts = new ArrayList<Bucket>(hist.dists.get(fld));
			dists.put(fld, bkts);
		}
	}

	public Set<String> fields() {
		return dists.keySet();
	}

	public Collection<Bucket> buckets(String fldName) {
		return dists.get(fldName);
	}

	public void addField(String fldName) {
		if (!dists.containsKey(fldName))
			dists.put(fldName, new ArrayList<Bucket>());
	}

	public void addBucket(String fldName, Bucket bkt) {
		if (!dists.containsKey(fldName))
			addField(fldName);
		dists.get(fldName).add(bkt);
	}

	/**
	 * Resets the buckets of given field.
	 * 
	 * @param fldName
	 *            the specified field
	 * @param bkts
	 */
	public void setBuckets(String fldName, Collection<Bucket> bkts) {
		if (!dists.containsKey(fldName))
			addField(fldName);
		dists.get(fldName).clear();
		if (bkts != null)
			dists.get(fldName).addAll(bkts);
	}

	/**
	 * Returns the estimated number of records output by the table approximated
	 * by this histogram.
	 * 
	 * @return the estimated number of records output by the table
	 */
	public double recordsOutput() {
		String fld = dists.keySet().iterator().next();
		double sum = 0.0;
		for (Bucket bkt : buckets(fld))
			sum += bkt.frequency();
		return sum;
	}

	/**
	 * Returns the estimated number of distinct values in the specified field of
	 * the table approximated by this histogram.
	 * 
	 * @param fldName
	 *            the specified field
	 * @return the estimated number of distinct values in the specified field
	 */
	public double distinctValues(String fldName) {
		double sum = 0;
		for (Bucket bkt : dists.get(fldName))
			sum += bkt.distinctValues();
		return sum;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("numRecs: " + String.format("%1$.1f", recordsOutput()));
		for (String fld : new TreeSet<String>(fields())) {
			sb.append(",\n" + fld + ": [");
			boolean fstBkt = true;
			for (Bucket bkt : buckets(fld)) {
				if (fstBkt)
					fstBkt = false;
				else
					sb.append(", ");
				sb.append(bkt.toString());
			}
			sb.append("]");
		}
		return sb.toString();
	}

	public String toString(int indents) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < indents; i++)
			sb.append("\t");
		String idt = sb.toString();
		String mIdt = idt + "\t";

		sb = new StringBuilder();
		sb.append("{");
		sb.append("\n" + mIdt + "numRecs: " + String.format("%1$.1f", recordsOutput()));
		for (String fld : new TreeSet<String>(fields())) {
			sb.append(",\n" + mIdt + fld + ": [");
			boolean fstBkt = true;
			for (Bucket bkt : buckets(fld)) {
				if (fstBkt)
					fstBkt = false;
				else
					sb.append(", ");
				sb.append(bkt.toString(indents + 1));
			}
			sb.append("]");
		}
		sb.append("\n" + idt + "}");
		return sb.toString();
	}
}
