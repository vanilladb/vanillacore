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
 * An expression consisting entirely of a single constant.
 */
public class ConstantExpression implements Expression {
	private Constant val;

	/**
	 * Creates a new expression by wrapping a constant.
	 * 
	 * @param c
	 *            the constant
	 */
	public ConstantExpression(Constant c) {
		val = c;
	}

	/**
	 * Returns true.
	 * 
	 * @see Expression#isConstant()
	 */
	@Override
	public boolean isConstant() {
		return true;
	}

	/**
	 * Returns false.
	 * 
	 * @see Expression#isFieldName()
	 */
	@Override
	public boolean isFieldName() {
		return false;
	}

	/**
	 * Unwraps the constant and returns it.
	 * 
	 * @see Expression#asConstant()
	 */
	@Override
	public Constant asConstant() {
		return val;
	}

	/**
	 * This method should never be called. Throws a ClassCastException.
	 * 
	 * @see Expression#asFieldName()
	 */
	@Override
	public String asFieldName() {
		throw new ClassCastException();
	}

	/**
	 * Returns the constant, regardless of the record.
	 * 
	 * @see Expression#evaluate(Record)
	 */
	@Override
	public Constant evaluate(Record rec) {
		return val;
	}

	/**
	 * Returns true, because a constant applies to any schema.
	 * 
	 * @see Expression#isApplicableTo
	 */
	@Override
	public boolean isApplicableTo(Schema sch) {
		return true;
	}

	/**
	 * Return constant value as a string. If the constant type is varchar, the
	 * output string will be surrounded by single quotation marks.
	 */
	@Override
	public String toString() {
		if (val.getType().isNumeric())
			return val.toString();
		else
			return "'" + val.toString() + "'";

	}
}
