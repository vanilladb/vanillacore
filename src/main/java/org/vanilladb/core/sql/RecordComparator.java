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
package org.vanilladb.core.sql;

import java.util.Comparator;
import java.util.List;

/**
 * A comparator for records.
 */
public class RecordComparator implements Comparator<Record> {
	public static final int DIR_ASC = 1, DIR_DESC = 2;

	private List<String> sortFlds;
	private List<Integer> sortDirs;

	/**
	 * Creates a comparator using the specified fields, using the ordering
	 * implied by its iterator.
	 * 
	 * @param sortFlds
	 *            the names of fields to compare by
	 * @param sortDirs
	 *            the sort directions corresponding to respective fields
	 */
	public RecordComparator(List<String> sortFlds, List<Integer> sortDirs) {
		this.sortFlds = sortFlds;
		this.sortDirs = sortDirs;
	}

	/**
	 * Compares two records and returns an integer less than (resp., equal to,
	 * or greater than) zero if the first record should be placed before (resp.,
	 * the same with, or after) the second record by following the previous
	 * specified directions.
	 * 
	 * <p>
	 * The sort fields are considered in turn. When a field is encountered for
	 * which the records have different values, those values are used as the
	 * result of the comparison. If the two records have the same values for all
	 * sort fields, then the method returns 0.
	 * </p>
	 * 
	 * @param rec1
	 *            the first scan
	 * @param rec2
	 *            the second scan
	 * @return the result of the comparison
	 */
	@Override
	public int compare(Record rec1, Record rec2) {
		for (int i = 0; i < sortFlds.size(); i++) {
			String fld = sortFlds.get(i);
			int dir = sortDirs.get(i);
			Constant val1 = rec1.getVal(fld);
			Constant val2 = rec2.getVal(fld);
			int result = val1.compareTo(val2);
			if (result != 0)
				return dir == DIR_ASC ? result : -result;
		}
		return 0;
	}
}
