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
package org.vanilladb.core.query.algebra.index;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchRange;

/**
 * The scan class corresponding to the indexjoin relational algebra operator.
 * The code is very similar to that of ProductScan, which makes sense because an
 * index join is essentially the product of each LHS record with the matching
 * RHS index records.
 */
public class IndexJoinScan implements Scan {
	private Scan s;
	private TableScan ts; // the data table
	private Index idx;
	private Map<String, String> joinFields; // <LHS field -> RHS field>
	private boolean isLhsEmpty;

	/**
	 * Creates an index join scan for the specified LHS scan and RHS index.
	 * 
	 * @param s
	 *            the LHS scan
	 * @param idx
	 *            the RHS index
	 * @param joinFields
	 *            the mapping of join fields from LHS to RHS
	 * @param ts
	 *            the table scan of data table
	 */
	public IndexJoinScan(Scan s, Index idx, Map<String, String> joinFields, TableScan ts) {
		this.s = s;
		this.idx = idx;
		this.joinFields = joinFields;
		this.ts = ts;
	}

	/**
	 * Positions the scan before the first record. That is, the LHS scan will be
	 * positioned at its first record, and the index will be positioned before
	 * the first record for the join value.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		s.beforeFirst();
		isLhsEmpty = !s.next();// in the case that s may be empty
		if (!isLhsEmpty)
			resetIndex();
	}

	/**
	 * Moves the scan to the next record. The method moves to the next index
	 * record, if possible. Otherwise, it moves to the next LHS record and the
	 * first index record. If there are no more LHS records, the method returns
	 * false.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		if (isLhsEmpty)
			return false;
		if (idx.next()) {
			ts.moveToRecordId(idx.getDataRecordId());
			return true;
		} else if (!(isLhsEmpty = !s.next())) {
			resetIndex();
			return next();
		} else
			return false;
	}

	/**
	 * Closes the scan by closing its LHS scan and its RHS index.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		s.close();
		idx.close();
		ts.close();
	}

	/**
	 * Returns the Constant value of the specified field.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		if (ts.hasField(fldName))
			return ts.getVal(fldName);
		else
			return s.getVal(fldName);
	}

	/**
	 * Returns true if the field is in the schema.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return ts.hasField(fldName) || s.hasField(fldName);
	}

	private void resetIndex() {
		Map<String, ConstantRange> ranges = new HashMap<String, ConstantRange>();
		
		for (Map.Entry<String, String> fieldPair : joinFields.entrySet()) {
			String lhsField = fieldPair.getKey();
			String rhsField = fieldPair.getValue();
			Constant commonVal = s.getVal(lhsField);
			ConstantRange range = ConstantRange.newInstance(commonVal);
			ranges.put(rhsField, range);
		}
		
		SearchRange searchRange = new SearchRange(idx.getIndexInfo().fieldNames(),
				idx.getKeyType(), ranges);
		idx.beforeFirst(searchRange);
	}

}
