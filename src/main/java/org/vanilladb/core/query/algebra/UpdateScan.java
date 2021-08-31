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
import org.vanilladb.core.storage.record.RecordId;

/**
 * The interface implemented by all updatable scans.
 */
public interface UpdateScan extends Scan {
	/**
	 * Modifies the field value of the current record.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @param val
	 *            the new value, expressed as a Constant
	 */
	void setVal(String fldName, Constant val);

	/**
	 * Inserts a new record somewhere in the scan.
	 */
	void insert();

	/**
	 * Deletes the current record from the scan.
	 */
	void delete();

	/**
	 * Returns the RecordId of the current record.
	 * 
	 * @return the RecordId of the current record
	 */
	RecordId getRecordId();

	/**
	 * Positions the scan so that the current record has the specified record ID
	 * .
	 * 
	 * @param rid
	 *            the RecordId of the desired record
	 */
	void moveToRecordId(RecordId rid);
}
