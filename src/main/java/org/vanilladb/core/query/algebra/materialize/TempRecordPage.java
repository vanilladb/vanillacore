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

import static org.vanilladb.core.sql.RecordComparator.DIR_ASC;

import java.util.List;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.Transaction;

public class TempRecordPage extends RecordPage {
	private Schema sch;

	public TempRecordPage(BlockId blk, TableInfo ti, Transaction tx) {
		super(blk, ti, tx, false);
		this.sch = ti.schema();
	}

	/**
	 * Insert records to TempRecordFile for sorting, at most one block long
	 * 
	 * @param s
	 *            the source scan
	 * @return true if another record is inserted false if block is full of
	 *         records
	 */
	public int insertFromScan(Scan s) {
		if (!super.insertIntoNextEmptySlot()) {
			return 0;
		}
		for (String fldName : sch.fields()) {
			Constant val = s.getVal(fldName);
			this.setVal(fldName, val);
		}
		if (s.next())
			return 1;
		else
			return -1;
	}

	/**
	 * Copy sorted records to UpdateScan
	 * 
	 * @param s
	 *            the target scan
	 * @return true if still record in TempRecordPage
	 */
	public boolean copyToScan(UpdateScan s) {
		if (!this.next())
			return false;
		s.insert();
		for (String fldName : sch.fields()) {
			s.setVal(fldName, this.getVal(fldName));
		}
		return true;
	}

	public void moveToPageHead() {
		this.moveToId(-1);
	}

	/**
	 * Selection sort. The values of sort directions are defined in
	 * {@link RecordComparator}.
	 * 
	 * @param sortFlds
	 *            the list of sorted fields
	 * @param sortDirs
	 *            the list of sorting directions
	 */
	public void sortbyselection(List<String> sortFlds, List<Integer> sortDirs) {
		moveToId(-1);
		int i = 0;
		while (super.next()) {
			int minId = findSmallestFrom(i, sortFlds, sortDirs);
			if (minId != i) {
				swapRecords(i, minId);
			}
			moveToId(i);
			i++;
		}
	}

	/**
	 * Scan id larger than startId to find smallest record (included startId)
	 * 
	 * @param startId
	 * @param sortFlds
	 * @param sortDirs
	 * @return the id of smallest record
	 */
	private int findSmallestFrom(int startId, List<String> sortFlds, List<Integer> sortDirs) {
		int minId = startId;
		moveToId(startId);
		while (super.next()) {
			int id = currentId();
			if (minId < 0 || compareRecords(minId, id, sortFlds, sortDirs) > 0)
				minId = id;
			moveToId(id);
		}
		return minId;
	}

	private void swapRecords(int id1, int id2) {
		for (String fldName : sch.fields()) {
			moveToId(id1);
			Constant val1 = getVal(fldName);
			moveToId(id2);
			Constant val2 = getVal(fldName);
			setVal(fldName, val1);
			moveToId(id1);
			setVal(fldName, val2);
		}
	}

	private int compareRecords(int id1, int id2, List<String> sortFlds, List<Integer> sortDirs) {
		for (int i = 0; i < sortFlds.size(); i++) {
			int dir = sortDirs.get(i);
			String fldName = sortFlds.get(i);
			moveToId(id1);
			Constant val1 = getVal(fldName);
			moveToId(id2);
			Constant val2 = getVal(fldName);
			int result = val1.compareTo(val2);
			if (result != 0)
				return dir == DIR_ASC ? result : -result;
		}
		return 0;
	}

}
