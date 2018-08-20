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
package org.vanilladb.core.query.parse;

import java.util.List;

import org.vanilladb.core.sql.Constant;

/**
 * Data for the SQL <em>insert</em> statement.
 */
public class InsertData {
	private String tblName;
	private List<String> fields;
	private List<Constant> vals;

	/**
	 * Saves the table name and the field and value lists.
	 * 
	 * @param tblName
	 *            the name of the affected table
	 * @param fields
	 *            a list of field names
	 * @param vals
	 *            a list of Constant values.
	 */
	public InsertData(String tblName, List<String> fields, List<Constant> vals) {
		this.tblName = tblName;
		this.fields = fields;
		this.vals = vals;
	}

	/**
	 * Returns the name of the affected table.
	 * 
	 * @return the name of the affected table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns a list of fields for which values will be specified in the new
	 * record.
	 * 
	 * @return a list of field names
	 */
	public List<String> fields() {
		return fields;
	}

	/**
	 * Returns a list of values for the specified fields. There is a one-one
	 * correspondence between this list of values and the list of fields.
	 * 
	 * @return a list of Constant values.
	 */
	public List<Constant> vals() {
		return vals;
	}
}
