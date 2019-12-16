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

import java.util.Arrays;
import java.util.List;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.storage.record.RecordId;

/**
 * The Scan class for the <em>sort</em> operator.
 * 
 */
public class SortScan implements Scan {
	private UpdateScan s1, s2 = null, currentScan = null;
	private RecordComparator comp;
	private boolean hasMore1, hasMore2 = false;
	private List<RecordId> savedPosition;

	/**
	 * Creates a sort scan, given a list of 1 or 2 runs. If there is only 1 run,
	 * then s2 will be null and hasMore2 will be false.
	 * 
	 * @param runs
	 *            the list of runs
	 * @param comp
	 *            the record comparator
	 */
	public SortScan(List<TempTable> runs, RecordComparator comp) {
		this.comp = comp;
		s1 = (UpdateScan) runs.get(0).open();
		if (runs.size() > 1)
			s2 = (UpdateScan) runs.get(1).open();
	}

	/**
	 * Positions the scan before the first record in sorted order. Internally,
	 * it moves to the first record of each underlying scan. The variable
	 * currentScan is set to null, indicating that there is no current scan.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		currentScan = null;
		s1.beforeFirst();
		hasMore1 = s1.next();
		if (s2 != null) {
			s2.beforeFirst();
			hasMore2 = s2.next();
		}
	}

	/**
	 * Moves to the next record in sorted order. First, the current scan is
	 * moved to the next record. Then the lowest record of the two scans is
	 * found, and that scan is chosen to be the new current scan.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		if (currentScan != null) {
			if (currentScan == s1)
				hasMore1 = s1.next();
			else if (currentScan == s2)
				hasMore2 = s2.next();
		}

		if (!hasMore1 && !hasMore2)
			return false;
		else if (hasMore1 && hasMore2) {
			// update currentScan
			currentScan = comp.compare(s1, s2) < 0 ? s1 : s2;
		} else if (hasMore1)
			currentScan = s1;
		else if (hasMore2)
			currentScan = s2;
		return true;
	}

	/**
	 * Closes the two underlying scans.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		s1.close();
		if (s2 != null)
			s2.close();
	}

	/**
	 * Gets the Constant value of the specified field of the current scan.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		return currentScan.getVal(fldName);
	}

	/**
	 * Returns true if the specified field is in the current scan.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return currentScan.hasField(fldName);
	}

	/**
	 * Saves the position of the current record, so that it can be restored at a
	 * later time.
	 */
	public void savePosition() {
		RecordId rid1 = s1.getRecordId();
		RecordId rid2 = (s2 == null) ? null : s2.getRecordId();
		savedPosition = Arrays.asList(rid1, rid2);
	}

	/**
	 * Moves the scan to its previously-saved position.
	 */
	public void restorePosition() {
		RecordId rid1 = savedPosition.get(0);
		RecordId rid2 = savedPosition.get(1);
		s1.moveToRecordId(rid1);
		if (rid2 != null)
			s2.moveToRecordId(rid2);
	}
}
