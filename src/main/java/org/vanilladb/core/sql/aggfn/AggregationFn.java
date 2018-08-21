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

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

/**
 * The interface implemented by aggregation functions.
 * <p>
 * Note that the aggregation function should implement {@link Object#hashCode()}
 * and {@link Object#equals(Object)} which are used to verify the equality of
 * aggregation functions.
 * </p>
 */
public abstract class AggregationFn {
	/**
	 * Processes the specified record by regarding it as the first record in a
	 * group.
	 * 
	 * @param rec
	 *            a record to aggregate over.
	 */
	public abstract void processFirst(Record rec);

	/**
	 * Processes the specified record by regarding it as a following up record
	 * in a group.
	 * 
	 * @param rec
	 *            a rec to aggregate over.
	 */
	public abstract void processNext(Record rec);

	/**
	 * Returns the name of the argument field.
	 * 
	 * @return the name of the argument field
	 */
	public abstract String argumentFieldName();

	/**
	 * Returns the name of the new aggregation field.
	 * 
	 * @return the name of the new aggregation field
	 */
	public abstract String fieldName();

	/**
	 * Returns the computed aggregation value given the records processed
	 * previously.
	 * 
	 * @return the computed aggregation value
	 */
	public abstract Constant value();

	/**
	 * Returns the type of aggregation value.
	 * 
	 * @return the type of aggregation value
	 */
	public abstract Type fieldType();

	/**
	 * Returns true if the type of aggregation value is depend on the argument
	 * field.
	 * 
	 * @return true if the type of aggregation value is depend on the argument
	 *         field
	 */
	public abstract boolean isArgumentTypeDependent();

	/**
	 * Returns a hash code value for the object.
	 */
	@Override
	public abstract int hashCode();

	/**
	 * Returns a hash code value for the object.
	 */
	@Override
	public abstract boolean equals(Object other);
}
