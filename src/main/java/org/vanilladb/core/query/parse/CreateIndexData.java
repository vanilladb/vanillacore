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
package org.vanilladb.core.query.parse;

/**
 * The parser for the <em>create index</em> statement.
 */
public class CreateIndexData {
	private String idxName, tblName, fldName;
	private int idxType;

	/**
	 * Saves the index type, table and field names of the specified index.
	 * 
	 * @param idxName
	 *            the name of the index
	 * @param tblName
	 *            the name of the indexed table
	 * @param fldName
	 *            the name of the indexed field
	 * @param idxType
	 *            the type of the index
	 */
	public CreateIndexData(String idxName, String tblName, String fldName, int idxType) {
		this.idxName = idxName;
		this.tblName = tblName;
		this.fldName = fldName;
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
	 * Returns the name of the indexed field.
	 * 
	 * @return the name of the indexed field
	 */
	public String fieldName() {
		return fldName;
	}

	/**
	 * Returns the type of the index.
	 * 
	 * @return the type of the index
	 */
	public int indexType() {
		return idxType;
	}
}
