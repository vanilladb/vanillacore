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

import org.vanilladb.core.sql.ConstantRange;

/**
 * A bucket in a histogram that holds the record frequency and other statistics
 * of a range of values in a specific field. Instances are immutable.
 */
public class Bucket {
	private ConstantRange valRange;
	private double freq;
	private double distVals;
	private Percentiles pcts;

	public Bucket(ConstantRange valRange, double freq, double distVals) {
		this.valRange = valRange;
		this.freq = freq;
		this.distVals = distVals;
	}

	public Bucket(ConstantRange valRange, double freq, double distVals,
			Percentiles pcts) {
		this.valRange = valRange;
		this.freq = freq;
		this.distVals = distVals;
		this.pcts = pcts;
	}

	public ConstantRange valueRange() {
		return valRange;
	}

	/**
	 * Returns the estimated frequency of records whose values fall within this
	 * instance's range.
	 * 
	 * @return the estimated frequency of the records whose values fall within
	 *         this instance's range
	 */
	public double frequency() {
		return freq;
	}

	/**
	 * Returns the estimated number of distinct values within this instance's
	 * range.
	 * 
	 * @return the estimated number of distinct values within this instance's
	 *         range
	 */
	public double distinctValues() {
		return distVals;
	}

	/**
	 * Returns the estimated number of distinct values within the overlap of
	 * this instance's range and the specified range.
	 * <p>
	 * Assumes that:
	 * <ul>
	 * <li>There is always a matching value if the specified range overlaps the
	 * range of this bucket</li>
	 * <li>Values are uniformly distributed within this instance's range if
	 * percentiles are not provided.</li>
	 * </ul>
	 * 
	 * @param range
	 *            the specified range
	 * @return the estimated number of distinct values within the overlap of
	 *         this instance's range and the specified range. No less than 1.0
	 *         if the range overlaps the range of this bucket.
	 */
	public double distinctValues(ConstantRange range) {
		ConstantRange overlap = valRange.intersect(range);
		if (!overlap.isValid())
			return 0.0;

		if (overlap.isConstant())
			return 1.0;
		double ret = distVals;
		if (pcts != null)
			ret *= pcts.percentage(range);
		else {
			if (valRange.isLowInclusive()
					&& (overlap.low().compareTo(valRange.low()) > 0 || !range
							.isLowInclusive()))
				ret -= 1.0;
			if (valRange.isHighInclusive()
					&& (overlap.high().compareTo(valRange.high()) < 0 || !range
							.isHighInclusive()))
				ret -= 1.0;
			ret *= overlap.length() / valRange.length();
		}
		return Math.max(ret, 1.0);
	}

	public Percentiles valuePercentiles() {
		return pcts;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("freq: " + String.format("%1$.1f", freq));
		sb.append(", valRange: " + valRange);
		sb.append(", distVals: " + distVals);
		sb.append(", pcts: " + (pcts == null ? "null" : pcts.toString()));
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
		sb.append("\n" + mIdt + "freq: " + String.format("%1$.1f", freq));
		sb.append(",\n" + mIdt + "valRange: " + valRange);
		sb.append(",\n" + mIdt + "distVals: " + distVals);
		sb.append(",\n" + mIdt + "pcts: "
				+ (pcts == null ? "null" : pcts.toString(indents + 1)));
		sb.append("\n" + idt + "}");
		return sb.toString();
	}
}
