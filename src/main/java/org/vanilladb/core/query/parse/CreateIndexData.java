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
package org.vanilladb.core.query.parse;

import java.util.List;

import org.vanilladb.core.storage.index.IndexType;

/**
 * The parser for the <em>create index</em> statement.
 */
public class CreateIndexData {
	private String idxName, tblName;
	private List<String> fldNames;
	private IndexType idxType;

	/**
	 * Saves the index type, table and field names of the specified index.
	 * 
	 * @param idxName
	 *            the name of the index.
	 * @param tblName
	 *            the name of the indexed table.
	 * @param fldNames
	 *            the list of the indexed fields.
	 * @param idxType
	 *            the type of the index.
	 */
	public CreateIndexData(String idxName, String tblName, List<String> fldNames, IndexType idxType) {
		this.idxName = idxName;
		this.tblName = tblName;
		this.fldNames = fldNames;
		this.idxType = idxType;
	}

	/**
	 * Returns the name of the index.
	 * 
	 * @return the name of the index
	 */
	public String indexName() {
		return idxName;
	}

	/**
	 * Returns the name of the indexed table.
	 * 
	 * @return the name of the indexed table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns the list of names of the indexed fields.
	 * 
	 * @return the list of names of the indexed fields
	 */
	public List<String> fieldNames() {
		return fldNames;
	}

	/**
	 * Returns the type of the index.
	 * 
	 * @return the type of the index
	 */
	public IndexType indexType() {
		return idxType;
	}
}
