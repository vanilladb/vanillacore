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

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;

/**
 * Keeps the percentiles of values in a bucket. For example, a percentile with
 * value v and percentage p% means there are at least p% of values which are
 * less than or equal to v in this bucket. Instances are immutable.
 */
public class Percentiles {
	private Map<Constant, Double> pcts;

	/**
	 * Constructs a new instance.
	 * 
	 * @param pcts
	 *            the percentiles and their corresponding percentages. Must
	 *            include the 100% percentile.
	 */
	public Percentiles(Map<Constant, Double> pcts) {
		this.pcts = new HashMap<Constant, Double>(pcts);
	}

	public SortedSet<Constant> values() {
		return new TreeSet<Constant>(pcts.keySet());
	}

	/**
	 * Gets the percent of values below the specified value.
	 * 
	 * @param v
	 *            the field value
	 * @return percentage (within [0, 1]) of values below the specified value.
	 */
	public double percentage(Constant v) {
		if (pcts.containsKey(v))
			return pcts.get(v);
		SortedSet<Constant> sorted = values();
		if (v.compareTo(sorted.first()) < 0)
			return 0.0;
		Constant prev = null;
		for (Constant pct : sorted) {
			if (prev != null && v.compareTo(prev) >= 0 && v.compareTo(pct) < 0)
				return pcts.get(prev);
			prev = pct;
		}
		return pcts.get(sorted.last());
	}

	/**
	 * Gets the percent of values within the specified value range.
	 * 
	 * @param range
	 *            the value range
	 * @return percentage (within [0, 1]) of values within the specified value
	 *         range
	 */
	public double percentage(ConstantRange range) {
		SortedSet<Constant> sorted = values();
		SortedSet<Constant> contained = new TreeSet<Constant>();
		Constant prev = null; // percentile before the first contained
		for (Constant pct : sorted) {
			if (range.contains(pct))
				contained.add(pct);
			else if (contained.isEmpty())
				prev = pct;
		}
		if (contained.size() == 0)
			return 0.0;
		double prevPercent = prev == null ? 0.0 : pcts.get(prev);
		return pcts.get(contained.last()) - prevPercent;
	}

	/**
	 * Returns a new instance that keeps only the percentiles with values
	 * falling within the specified range. All new percentiles are adjusted such
	 * that the last value has the 100% percentage.
	 * 
	 * @param range
	 *            the range within which the percentiles will be preserved
	 * @return a new instance that keeps only the percentiles falling within the
	 *         specified range; or <code>null</code> if no such a percentile
	 */
	public Percentiles percentiles(ConstantRange range) {
		SortedSet<Constant> sorted = values();
		SortedSet<Constant> contained = new TreeSet<Constant>();
		Constant prev = null;
		for (Constant pct : sorted) {
			if (range.contains(pct)) {
				contained.add(pct);
			} else if (contained.isEmpty())
				prev = pct;
		}
		if (contained.isEmpty())
			return null;
		double prevPercent = prev == null ? 0.0 : pcts.get(prev);
		double totalPercent = pcts.get(contained.last()) - prevPercent;
		Map<Constant, Double> newPcts = new HashMap<Constant, Double>();
		for (Constant p : contained)
			newPcts.put(p, (pcts.get(p) - prevPercent) / totalPercent);
		return new Percentiles(newPcts);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		boolean fstP = true;
		TreeSet<Constant> ps = new TreeSet<Constant>(pcts.keySet());
		for (Constant p : ps) {
			if (fstP)
				fstP = false;
			else
				sb.append(", ");
			sb.append(p + ": " + String.format("%1$.2f", pcts.get(p)));
		}
		sb.append("}");
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
		boolean fstP = true;
		TreeSet<Constant> ps = new TreeSet<Constant>(pcts.keySet());
		for (Constant p : ps) {
			if (fstP)
				fstP = false;
			else
				sb.append(",");
			sb.append("\n" + mIdt + p + ": "
					+ String.format("%1$.2f", pcts.get(p)));
		}
		sb.append("\n" + idt + "}");
		return sb.toString();
	}
}
