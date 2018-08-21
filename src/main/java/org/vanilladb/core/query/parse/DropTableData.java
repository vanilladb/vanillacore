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

/**
 * Data for the SQL <em>drop table</em> statement.
 */
public class DropTableData {
	private String tblName;

	/**
	 * Saves the table name and schema.
	 * 
	 * @param tblName
	 *            the name of the new table
	 */
	public DropTableData(String tblName) {
		this.tblName = tblName;
	}

	/**
	 * Returns the name of the new table.
	 * 
	 * @return the name of the new table
	 */
	public String tableName() {
		return tblName;
	}
}
