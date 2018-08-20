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

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.storage.record.RecordId;

/**
 * The scan class corresponding to the <em>select</em> relational algebra
 * operator. All methods except next delegate their work to the underlying scan.
 */
public class SelectScan implements UpdateScan {
	private Scan s;
	private Predicate pred;

	/**
	 * Creates a select scan having the specified underlying scan and predicate.
	 * 
	 * @param s
	 *            the scan of the underlying query
	 * @param pred
	 *            the selection predicate
	 */
	public SelectScan(Scan s, Predicate pred) {
		this.s = s;
		this.pred = pred;
	}

	// Scan methods

	@Override
	public void beforeFirst() {
		s.beforeFirst();
	}

	/**
	 * Move to the next record satisfying the predicate. The method repeatedly
	 * calls next on the underlying scan until a suitable record is found, or
	 * the underlying scan contains no more records.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		while (s.next())
			if (pred.isSatisfied(s))
				return true;
		return false;
	}

	@Override
	public void close() {
		s.close();
	}

	@Override
	public Constant getVal(String fldName) {
		return s.getVal(fldName);
	}

	@Override
	public boolean hasField(String fldName) {
		return s.hasField(fldName);
	}

	// UpdateScan methods

	@Override
	public void setVal(String fldname, Constant val) {
		UpdateScan us = (UpdateScan) s;
		us.setVal(fldname, val);
	}

	@Override
	public void delete() {
		UpdateScan us = (UpdateScan) s;
		us.delete();
	}

	@Override
	public void insert() {
		UpdateScan us = (UpdateScan) s;
		us.insert();
	}

	@Override
	public RecordId getRecordId() {
		UpdateScan us = (UpdateScan) s;
		return us.getRecordId();
	}

	@Override
	public void moveToRecordId(RecordId rid) {
		UpdateScan us = (UpdateScan) s;
		us.moveToRecordId(rid);
	}
}
