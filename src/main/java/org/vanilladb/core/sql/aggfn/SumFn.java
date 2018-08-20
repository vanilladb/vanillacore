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
package org.vanilladb.core.sql.aggfn;

import static org.vanilladb.core.sql.Type.DOUBLE;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

/**
 * The <em>sum</em> aggregation function.
 */
public class SumFn extends AggregationFn {
	private String fldName;
	private Constant val;

	public SumFn(String fldName) {
		this.fldName = fldName;
	}

	@Override
	public void processFirst(Record rec) {
		Constant c = rec.getVal(fldName);
		this.val = c.castTo(DOUBLE);
	}

	@Override
	public void processNext(Record rec) {
		Constant newval = rec.getVal(fldName);
		val = val.add(newval);
	}

	@Override
	public String argumentFieldName() {
		return fldName;
	}

	@Override
	public String fieldName() {
		return "sumof" + fldName;
	}

	@Override
	public Constant value() {
		return val;
	}

	@Override
	public Type fieldType() {
		return DOUBLE;
	}

	@Override
	public boolean isArgumentTypeDependent() {
		return false;
	}

	@Override
	public int hashCode() {
		return fieldName().hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (this == other)
			return true;

		if (!(other.getClass().equals(SumFn.class)))
			return false;

		SumFn otherSumFn = (SumFn) other;
		if (!fldName.equals(otherSumFn.fldName))
			return false;

		return true;
	}
}
