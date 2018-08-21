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

import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.HashSet;
import java.util.Set;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

/**
 * The <em>distinct count</em> aggregation function.
 */
public class DistinctCountFn extends AggregationFn {
	private String fldName;
	private Set<Constant> distValues;

	public DistinctCountFn(String fldName) {
		this.fldName = fldName;
		this.distValues = new HashSet<Constant>();
	}

	@Override
	public void processFirst(Record rec) {
		if (distValues.size() != 0)
			distValues.clear();
		distValues.add(rec.getVal(fldName));
	}

	@Override
	public void processNext(Record rec) {
		distValues.add(rec.getVal(fldName));
	}

	@Override
	public String argumentFieldName() {
		return fldName;
	}

	@Override
	public String fieldName() {
		return "dstcountof" + fldName;
	}

	@Override
	public Constant value() {
		return new IntegerConstant(distValues.size());
	}

	@Override
	public Type fieldType() {
		return INTEGER;
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

		if (!(other.getClass().equals(DistinctCountFn.class)))
			return false;

		DistinctCountFn otherDistinctCountFn = (DistinctCountFn) other;
		if (!fldName.equals(otherDistinctCountFn.fldName))
			return false;

		return true;
	}
}
