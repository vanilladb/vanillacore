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
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;

/**
 * The scan class corresponding to the <em>explain</em> relational algebra
 * operator.
 */
public class ExplainScan implements Scan {

	private String result;
	private int numRecs;
	private Schema schema;
	private boolean isBeforeFirsted;

	/**
	 * Creates a explain scan having the specified underlying query.
	 * 
	 * @param s
	 *            the scan of the underlying query
	 * @param schema
	 *            the schema of the explain result
	 * @param explain
	 *            the string that explains the underlying query's planning tree
	 */
	public ExplainScan(Scan s, Schema schema, String explain) {
		this.result = "\n" + explain;
		this.schema = schema;
		s.beforeFirst();
		while (s.next())
			numRecs++;
		s.close();
		this.result = result + "\nActual #recs: " + numRecs;
		isBeforeFirsted = true;
	}

	@Override
	public Constant getVal(String fldName) {
		if (fldName.equals("query-plan")) {
			return new VarcharConstant(result);
		} else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	@Override
	public void beforeFirst() {
		isBeforeFirsted = true;
	}

	@Override
	public boolean next() {
		if (isBeforeFirsted) {
			isBeforeFirsted = false;
			return true;
		} else
			return false;
	}

	@Override
	public void close() {
		// do nothing
	}

	@Override
	public boolean hasField(String fldname) {
		return schema.hasField(fldname);
	}
}
