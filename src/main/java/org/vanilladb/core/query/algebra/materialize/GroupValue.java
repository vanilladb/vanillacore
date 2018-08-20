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
import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;

/**
 * An object that holds the values of the grouping fields for the current record
 * of a scan.
 */
public class GroupValue {
	private Map<String, Constant> vals;

	/**
	 * Creates a new group value, given the specified scan and list of fields.
	 * The values in the current record of each field are stored.
	 * 
	 * @param s
	 *            a scan
	 * @param groupFlds
	 *            the fields to group by. Can be empty, which means that all
	 *            records are in a single group.
	 */
	public GroupValue(Scan s, Collection<String> groupFlds) {
		vals = new HashMap<String, Constant>();
		for (String fldname : groupFlds)
			vals.put(fldname, s.getVal(fldname));
	}

	/**
	 * Returns the Constant value of the specified field in the group.
	 * 
	 * @param fldName
	 *            the name of a field
	 * @return the value of the field in the group
	 */
	public Constant getVal(String fldName) {
		return vals.get(fldName);
	}

	/**
	 * Two GroupValue objects are equal if they have the same values for their
	 * grouping fields.
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj.getClass().equals(GroupValue.class)))
			return false;
		GroupValue gv = (GroupValue) obj;
		for (String fldname : vals.keySet()) {
			Constant v1 = vals.get(fldname);
			Constant v2 = gv.getVal(fldname);
			if (!v1.equals(v2))
				return false;
		}
		// always returns true if vals is empty
		return true;
	}

	/**
	 * The hashcode of a GroupValue object is the sum of the hashcodes of its
	 * field values.
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		int hashval = 0;
		for (Constant c : vals.values())
			hashval += c.hashCode();
		return hashval;
	}
}
