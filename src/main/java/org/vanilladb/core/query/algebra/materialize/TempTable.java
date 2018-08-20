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
package org.vanilladb.core.query.algebra.materialize;

import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.FileHeaderFormatter;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * A class that creates temporary tables. A temporary table is not registered in
 * the catalog. The class therefore has a method getTableInfo to return the
 * table's metadata.
 */
public class TempTable {
	private static long nextTableNum = 0;
	private TableInfo ti;
	private Transaction tx;

	/**
	 * Allocates a name for for a new temporary table having the specified
	 * schema.
	 * 
	 * @param sch
	 *            the new table's schema
	 * @param tx
	 *            the calling transaction
	 */
	public TempTable(Schema sch, Transaction tx) {
		String tblname = nextTableName();
		ti = new TableInfo(tblname, sch);
		this.tx = tx;
		FileHeaderFormatter fhf = new FileHeaderFormatter();
		Buffer buff = tx.bufferMgr().pinNew(ti.fileName(), fhf);
		tx.bufferMgr().unpin(buff);
	}

	/**
	 * Opens a table scan for the temporary table.
	 * 
	 * @return the scan for the temporary table
	 */
	public UpdateScan open() {
		return new TableScan(ti, tx);
	}

	/**
	 * Return the table's metadata.
	 * 
	 * @return the table's metadata
	 */
	public TableInfo getTableInfo() {
		return ti;
	}

	private static synchronized String nextTableName() {
		nextTableNum++;
		return "_temp" + nextTableNum;
	}

}
