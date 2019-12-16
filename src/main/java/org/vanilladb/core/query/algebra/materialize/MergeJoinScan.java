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

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;

/**
 * The Scan class for the <em>mergejoin</em> operator.
 */
public class MergeJoinScan implements Scan {
	private SortScan ss1;
	private SortScan ss2;
	private String fldName1, fldName2;
	private Constant joinVal = null;

	/**
	 * Creates a mergejoin scan for the two underlying sorted scans.
	 * 
	 * @param ss1
	 *            the LHS sorted scan
	 * @param ss2
	 *            the RHS sorted scan
	 * @param fldName1
	 *            the LHS join field
	 * @param fldName2
	 *            the RHS join field
	 */
	public MergeJoinScan(SortScan ss1, SortScan ss2, String fldName1,
			String fldName2) {
		this.ss1 = ss1;
		this.ss2 = ss2;
		this.fldName1 = fldName1;
		this.fldName2 = fldName2;
	}

	/**
	 * Positions the scan before the first record, by positioning each
	 * underlying scan before their first records.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		ss1.beforeFirst();
		ss2.beforeFirst();
	}

	/**
	 * Closes the scan by closing the two underlying scans.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		ss1.close();
		ss2.close();
	}

	/**
	 * Moves to the next record. This is where the action is.
	 * <P>
	 * If the next RHS record has the same join value, then move to it.
	 * Otherwise, if the next LHS record has the same join value, then
	 * reposition the RHS scan back to the first record having that join value.
	 * Otherwise, repeatedly move the scan having the smallest value until a
	 * common join value is found. When one of the scans runs out of records,
	 * return false.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		boolean hasmore2 = ss2.next();
		if (hasmore2 && ss2.getVal(fldName2).equals(joinVal))
			return true;

		boolean hasmore1 = ss1.next();
		if (hasmore1 && ss1.getVal(fldName1).equals(joinVal)) {
			ss2.restorePosition();
			return true;
		}

		while (hasmore1 && hasmore2) {
			Constant v1 = ss1.getVal(fldName1);
			Constant v2 = ss2.getVal(fldName2);
			if (v1.compareTo(v2) < 0)
				hasmore1 = ss1.next();
			else if (v1.compareTo(v2) > 0)
				hasmore2 = ss2.next();
			else {
				ss2.savePosition();
				joinVal = ss2.getVal(fldName2);
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns the value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		if (ss1.hasField(fldName))
			return ss1.getVal(fldName);
		else
			return ss2.getVal(fldName);
	}

	/**
	 * Returns true if the specified field is in either of the underlying scans.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return ss1.hasField(fldName) || ss2.hasField(fldName);
	}
}
