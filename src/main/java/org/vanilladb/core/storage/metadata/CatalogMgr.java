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
package org.vanilladb.core.storage.metadata;

import java.util.Map;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.index.IndexMgr;
import org.vanilladb.core.storage.tx.Transaction;

public class CatalogMgr {
	private static TableMgr tblMgr;
	private static ViewMgr viewMgr;
	private static IndexMgr idxMgr;

	public CatalogMgr(boolean isNew, Transaction tx) {
		tblMgr = new TableMgr(isNew, tx);
		viewMgr = new ViewMgr(isNew, tblMgr, tx);
		idxMgr = new IndexMgr(isNew, tblMgr, tx);
	}

	public void createTable(String tblName, Schema sch, Transaction tx) {
		tblMgr.createTable(tblName, sch, tx);
	}

	public void dropTable(String tblName, Transaction tx) {
		tblMgr.dropTable(tblName, tx);
	}

	public TableInfo getTableInfo(String tblName, Transaction tx) {
		return tblMgr.getTableInfo(tblName, tx);
	}

	public void createView(String viewName, String viewDef, Transaction tx) {
		viewMgr.createView(viewName, viewDef, tx);
	}

	public void dropView(String viewName, Transaction tx) {
		viewMgr.dropView(viewName, tx);
	}

	public String getViewDef(String viewName, Transaction tx) {
		return viewMgr.getViewDef(viewName, tx);
	}

	public void createIndex(String idxName, String tblName, String fldName,
			int indexType, Transaction tx) {
		idxMgr.createIndex(idxName, tblName, fldName, indexType, tx);
	}

	public void dropIndex(String idxName, Transaction tx) {
		idxMgr.dropIndex(idxName, tx);
	}

	public Map<String, IndexInfo> getIndexInfo(String tblName, Transaction tx) {
		return idxMgr.getIndexInfo(tblName, tx);
	}
}
