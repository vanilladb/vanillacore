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
package org.vanilladb.core.storage.index;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;

public class SearchRange {

	private ConstantRange[] ranges;
	private SearchKey min, max;

	public SearchRange(List<String> indexedFields, Schema tblSch, Map<String, ConstantRange> specifiedRanges) {
		ranges = new ConstantRange[indexedFields.size()];
		Iterator<String> fldNameIter = indexedFields.iterator();
		String fldName;

		for (int i = 0; i < ranges.length; i++) {
			fldName = fldNameIter.next();
			ranges[i] = specifiedRanges.get(fldName);
			if (ranges[i] == null) {
				Type type = tblSch.type(fldName);
				ranges[i] = ConstantRange.newInstance(type.minValue(), true, type.maxValue(), true);
			}
		}
	}

	public SearchRange(List<String> indexedFields, SearchKeyType keyType, Map<String, ConstantRange> specifiedRanges) {
		ranges = new ConstantRange[indexedFields.size()];
		Iterator<String> fldNameIter = indexedFields.iterator();
		String fldName;

		for (int i = 0; i < ranges.length; i++) {
			fldName = fldNameIter.next();
			ranges[i] = specifiedRanges.get(fldName);
			if (ranges[i] == null) {
				Type type = keyType.get(i);
				ranges[i] = ConstantRange.newInstance(type.minValue(), true, type.maxValue(), true);
			}
		}
	}

	public SearchRange(SearchKey key) {
		ranges = new ConstantRange[key.length()];
		for (int i = 0; i < ranges.length; i++) {
			ranges[i] = ConstantRange.newInstance(key.get(i));
		}
	}

	public SearchRange(ConstantRange... constantRanges) {
		ranges = Arrays.copyOf(constantRanges, constantRanges.length);
	}

	public int length() {
		return ranges.length;
	}

	public ConstantRange get(int index) {
		return ranges[index];
	}

	@Override
	public String toString() {
		return Arrays.toString(ranges);
	}

	public boolean isValid() {
		for (ConstantRange range : ranges)
			if (!range.isValid())
				return false;
		return true;
	}

	public SearchKey getMin() {
		if (min != null)
			return min;

		Constant[] vals = new Constant[ranges.length];
		for (int i = 0; i < vals.length; i++)
			vals[i] = ranges[i].low();
		min = new SearchKey(vals);

		return min;
	}

	public SearchKey getMax() {
		if (max != null)
			return max;

		Constant[] vals = new Constant[ranges.length];
		for (int i = 0; i < vals.length; i++)
			vals[i] = ranges[i].high();
		max = new SearchKey(vals);

		return max;
	}

	/**
	 * Check if the given {@link SearchKey} matches this range.
	 * 
	 * @param key
	 *            the specified {@link SearchKey}
	 * @return if the given key matches
	 */
	public boolean match(SearchKey key) {
		// It will not check if the key does not have the same length
		if (ranges.length != key.length())
			return false;

		// Check one by one
		for (int i = 0; i < ranges.length; i++)
			if (!ranges[i].contains(key.get(i)))
				return false;
		return true;
	}

	/**
	 * Check if the given {@link SearchKey} is in the [min, max] of this range.
	 * Note that a SearchKey may be in the [min, max] but not match the range.
	 * <br>
	 * <br>
	 * For example, assume there is a SearchRange is {1, ALL, 5}. The min of the
	 * range is {1, -INT_MAX, 5} and the max of the range is {1, INT_MAX, 5}. A
	 * key {1, 2, 3} is the [min, max] but not match {1, ALL, 5}, due to the way
	 * of comparing SearchKeys.
	 * 
	 * @param key
	 *            the search key
	 * @return whether the key is in the range
	 */
	public boolean betweenMinAndMax(SearchKey key) {
		return key.compareTo(getMin()) >= 0 && key.compareTo(getMax()) <= 0;
	}

	/**
	 * Check if this range represents a single key. It is also used to check if
	 * this range can be converted into a {@link SearchKey}.
	 * 
	 * @return if this range represents a single key
	 */
	public boolean isSingleValue() {
		for (ConstantRange range : ranges)
			if (!range.isConstant())
				return false;
		return true;
	}

	/**
	 * Converts this range into a {@link SearchKey}. We expect the caller will
	 * call {@link #isSingleValue()} before calling this method.
	 * 
	 * @return a SearchKey represents the range
	 */
	public SearchKey asSearchKey() {
		Constant[] vals = new Constant[ranges.length];
		for (int i = 0; i < vals.length; i++)
			vals[i] = ranges[i].asConstant();
		return new SearchKey(vals);
	}
}
