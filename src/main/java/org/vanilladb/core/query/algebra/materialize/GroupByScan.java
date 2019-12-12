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

import java.util.Collection;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.aggfn.AggregationFn;

/**
 * The Scan class for the <em>groupby</em> operator.
 */
public class GroupByScan implements Scan {
	private Scan ss;
	private Collection<String> groupFlds;
	private Collection<AggregationFn> aggFns;
	private GroupValue groupVal;
	private boolean moreGroups;

	/**
	 * Creates a groupby scan, given a grouped table scan.
	 * 
	 * @param s
	 *            the sorted scan
	 * @param groupFlds
	 *            the fields to group by. Can be empty, which means that all
	 *            records are in a single group.
	 * @param aggFns
	 *            the aggregation functions
	 */
	public GroupByScan(Scan s, Collection<String> groupFlds,
			Collection<AggregationFn> aggFns) {
		this.ss = s;
		this.groupFlds = groupFlds;
		this.aggFns = aggFns;
	}

	/**
	 * Positions the scan before the first group. Internally, the underlying
	 * scan is always positioned at the first record of a group, which means
	 * that this method moves to the first underlying record.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		ss.beforeFirst();
		moreGroups = ss.next();
	}

	/**
	 * Moves to the next group. The key of the group is determined by the group
	 * values at the current record. The method repeatedly reads underlying
	 * records until it encounters a record having a different key. The
	 * aggregation functions are called for each record in the group. The values
	 * of the grouping fields for the group are saved.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		if (!moreGroups)
			return false;
		if (aggFns != null)
			for (AggregationFn fn : aggFns)
				fn.processFirst(ss);
		groupVal = new GroupValue(ss, groupFlds);
		while (moreGroups = ss.next()) {
			GroupValue gv = new GroupValue(ss, groupFlds);
			if (!groupVal.equals(gv))
				break;
			if (aggFns != null)
				for (AggregationFn fn : aggFns)
					fn.processNext(ss);
		}
		return true;
	}

	/**
	 * Closes the scan by closing the underlying scan.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		ss.close();
	}

	/**
	 * Gets the Constant value of the specified field. If the field is a group
	 * field, then its value can be obtained from the saved group value.
	 * Otherwise, the value is obtained from the appropriate aggregation
	 * function.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldname) {
		if (groupFlds.contains(fldname))
			return groupVal.getVal(fldname);
		if (aggFns != null)
			for (AggregationFn fn : aggFns)
				if (fn.fieldName().equals(fldname))
					return fn.value();
		throw new RuntimeException("field " + fldname + " not found.");
	}

	/**
	 * Returns true if the specified field is either a grouping field or created
	 * by an aggregation function.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldname) {
		if (groupFlds.contains(fldname))
			return true;
		if (aggFns != null)
			for (AggregationFn fn : aggFns)
				if (fn.fieldName().equals(fldname))
					return true;
		return false;
	}
}
