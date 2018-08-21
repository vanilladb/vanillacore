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

import org.vanilladb.core.sql.Schema;

/**
 * Data for the SQL <em>create table</em> statement.
 */
public class CreateTableData {
	private String tblName;
	private Schema schema;

	/**
	 * Saves the table name and schema.
	 * 
	 * @param tblName
	 *            the name of the new table
	 * @param schema
	 *            the schema of the new table
	 */
	public CreateTableData(String tblName, Schema schema) {
		this.tblName = tblName;
		this.schema = schema;
	}

	/**
	 * Returns the name of the new table.
	 * 
	 * @return the name of the new table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns the schema of the new table.
	 * 
	 * @return the schema of the new table
	 */
	public Schema newSchema() {
		return schema;
	}
}
