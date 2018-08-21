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

import org.vanilladb.core.sql.Record;

/**
 * The interface will be implemented by each query scan. There is a Scan class
 * for each relational algebra operator.
 * 
 * <p>
 * The {@link #beforeFirst()} method must be called before {@link #next()}.
 * </p>
 */
public interface Scan extends Record {

	/**
	 * Positions the scan before its first record.
	 */
	void beforeFirst();

	/**
	 * Moves the scan to the next record.
	 * 
	 * @return false if there is no next record
	 */
	boolean next();

	/**
	 * Closes the scan and its subscans, if any.
	 */
	void close();

	/**
	 * Returns true if the scan has the specified field.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return true if the scan has that field
	 */
	boolean hasField(String fldName);
}
