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
package org.vanilladb.core.sql.predicate;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;

/**
 * An expression consisting entirely of a single field.
 */
public class FieldNameExpression implements Expression {
	private String fldName;

	/**
	 * Creates a new expression by wrapping a field.
	 * 
	 * @param fldName
	 *            the name of the wrapped field
	 */
	public FieldNameExpression(String fldName) {
		this.fldName = fldName;
	}

	/**
	 * Returns false.
	 * 
	 * @see Expression#isConstant()
	 */
	@Override
	public boolean isConstant() {
		return false;
	}

	/**
	 * Returns true.
	 * 
	 * @see Expression#isFieldName()
	 */
	@Override
	public boolean isFieldName() {
		return true;
	}

	/**
	 * This method should never be called. Throws a ClassCastException.
	 * 
	 * @see Expression#asConstant()
	 */
	@Override
	public Constant asConstant() {
		throw new ClassCastException();
	}

	/**
	 * Unwraps the field name and returns it.
	 * 
	 * @see Expression#asFieldName()
	 */
	@Override
	public String asFieldName() {
		return fldName;
	}

	/**
	 * Evaluates the field by getting its value from the record.
	 * 
	 * @see Expression#evaluate(Record)
	 */
	@Override
	public Constant evaluate(Record rec) {
		return rec.getVal(fldName);
	}

	/**
	 * Returns true if the field is in the specified schema.
	 * 
	 * @see Expression#isApplicableTo(Schema)
	 */
	@Override
	public boolean isApplicableTo(Schema sch) {
		return sch.hasField(fldName);
	}

	@Override
	public String toString() {
		return fldName;
	}
}
