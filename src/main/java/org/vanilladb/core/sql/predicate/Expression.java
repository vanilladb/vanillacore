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
 * A SQL expression.
 */
public interface Expression {
	/**
	 * Returns true if the expression is a constant.
	 * 
	 * @return true if the expression is a constant
	 */
	boolean isConstant();

	/**
	 * Returns true if the expression is a field reference.
	 * 
	 * @return true if the expression denotes a field
	 */
	boolean isFieldName();

	/**
	 * Returns the constant corresponding to a constant expression. Throws an
	 * exception if this expression does not denote a constant.
	 * 
	 * @return the expression as a constant
	 */
	Constant asConstant();

	/**
	 * Returns the field name corresponding to a field name expression. Throws
	 * an exception if this expression does not denote a field.
	 * 
	 * @return the expression as a field name
	 */
	String asFieldName();

	/**
	 * Evaluates the expression with respect to the specified record.
	 * 
	 * @param rec
	 *            the record
	 * @return the value of the expression, as a constant
	 */
	Constant evaluate(Record rec);

	/**
	 * Determines if all of the fields mentioned in this expression are
	 * contained in the specified schema.
	 * 
	 * @param sch
	 *            the schema
	 * @return true if all fields in the expression are in the schema
	 */
	boolean isApplicableTo(Schema sch);
}
