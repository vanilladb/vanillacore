/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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
package org.vanilladb.core.storage.metadata;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The metadata about a table and its records.
 */
public class TableInfo {
	private Schema schema;
	private String tblName;

	/**
	 * Creates a TableInfo object, given a table name and schema. The
	 * constructor calculates the physical offset of each field. This
	 * constructor is used when a table is created.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param schema
	 *            the schema of the table's records
	 */
	public TableInfo(String tblName, Schema schema) {
		this.schema = schema;
		this.tblName = tblName;
	}

	/**
	 * Returns the filename assigned to this table. Currently, the filename is
	 * the table name followed by ".tbl".
	 * 
	 * @return the name of the file assigned to the table
	 */
	public String fileName() {
		return tblName + ".tbl";
	}

	/**
	 * Returns the table name of this TableInfo
	 * 
	 * @return the name of the file assigned to the table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns the schema of the table's records
	 * 
	 * @return the table's record schema
	 */
	public Schema schema() {
		return schema;
	}

	/**
	 * Opens the {@link RecordFile} described by this object.
	 * 
	 * @return the {@link RecordFile} object associated with this information
	 */

	/**
	 * Opens the {@link RecordFile} described by this object.
	 * 
	 * @return the {@link RecordFile} object associated with this information
	 * 
	 * @param tx
	 *            the context of executing transaction
	 * @param doLog
	 *            will the transaction log the modification
	 * @return the opened record file
	 */
	public RecordFile open(Transaction tx, boolean doLog) {
		return new RecordFile(this, tx, doLog);
	}
}
