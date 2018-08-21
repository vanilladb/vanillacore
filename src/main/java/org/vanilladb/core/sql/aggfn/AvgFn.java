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
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

/**
 * The <em>avg</em> aggregation function. Supports only a numeric field.
 */
public class AvgFn extends AggregationFn {
	private String fldName;
	private Constant sum;
	private int count;

	public AvgFn(String fldName) {
		this.fldName = fldName;
	}

	@Override
	public void processFirst(Record rec) {
		count = 1;
		this.sum = rec.getVal(fldName).castTo(DOUBLE);
	}

	@Override
	public void processNext(Record rec) {
		count++;
		sum = sum.add(rec.getVal(fldName));
	}

	@Override
	public String argumentFieldName() {
		return fldName;
	}

	@Override
	public String fieldName() {
		return "avgof" + fldName;
	}

	@Override
	public Constant value() {
		return sum.div(new DoubleConstant(this.count));
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

		if (!(other.getClass().equals(AvgFn.class)))
			return false;

		AvgFn otherAvgFn = (AvgFn) other;
		if (!fldName.equals(otherAvgFn.fldName))
			return false;

		return true;
	}
}
