/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.vanilladb.core.storage.metadata.index;

import java.util.List;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.TableNotFoundException;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The information about an index. This information is used by the query planner
 * in order to estimate the costs of using the index, and to obtain the schema
 * of the index records. Its methods are essentially the same as those of Plan.
 */
public class IndexInfo {
	private String idxName, tblName;
	private List<String> fldNames;
	private IndexType idxType;

	/**
	 * Creates an IndexInfo object for the specified index.
	 * 
	 * @param idxName
	 *            the name of the index
	 * @param tblName
	 *            the name of the table
	 * @param fldNames
	 *            the list of names of the indexed fields
	 * @param idxType
	 *            the type of the index
	 */
	public IndexInfo(String idxName, String tblName, List<String> fldNames, IndexType idxType) {
		this.tblName = tblName;
		this.idxName = idxName;
		this.fldNames = fldNames;
		this.idxType = idxType;
	}

	/**
	 * Opens the index described by this object.
	 * 
	 * @return the {@link Index} object associated with this information
	 */
	public Index open(Transaction tx) {
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
		if (ti == null)
			throw new TableNotFoundException("table '" + tblName
					+ "' is not defined in catalog.");

		return Index.newInstance(this, new SearchKeyType(ti.schema(), fldNames), tx);
	}

	/**
	 * Returns the names of the indexed fields.
	 * 
	 * @return the names of the indexed fields
	 */
	public List<String> fieldNames() {
		return fldNames;
	}

	/**
	 * Returns the table name of this IndexInfo.
	 * 
	 * @return the name of the indexed table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns the type of this IndexInfo.
	 * 
	 * @return the type of this index
	 */
	public IndexType indexType() {
		return idxType;
	}

	/**
	 * Returns the name of this index.
	 * 
	 * @return the name of this index
	 */
	public String indexName() {
		return idxName;
	}
}
