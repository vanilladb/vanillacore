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
package org.vanilladb.core.storage.metadata;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.IndexType;
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

	public Collection<String> getViewNamesByTable(String tblName, Transaction tx) {
		return viewMgr.getViewNamesByTable(tblName, tx);
	}

	public String getViewDef(String viewName, Transaction tx) {
		return viewMgr.getViewDef(viewName, tx);
	}

	public void createIndex(String idxName, String tblName, List<String> fldNames,
			IndexType indexType, Transaction tx) {
		idxMgr.createIndex(idxName, tblName, fldNames, indexType, tx);
	}

	public void dropIndex(String idxName, Transaction tx) {
		idxMgr.dropIndex(idxName, tx);
	}
	
	public Set<String> getIndexedFields(String tblName, Transaction tx) {
		return idxMgr.getIndexedFields(tblName, tx);
	}

	public List<IndexInfo> getIndexInfo(String tblName, String fldName, 
			Transaction tx) {
		return idxMgr.getIndexInfo(tblName, fldName, tx);
	}

	public IndexInfo getIndexInfoByName(String idxName, Transaction tx) {
		return idxMgr.getIndexInfoByName(idxName, tx);
	}
}
