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
package org.vanilladb.core.query.planner.index;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.DropIndexData;
import org.vanilladb.core.query.parse.DropTableData;
import org.vanilladb.core.query.parse.DropViewData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.planner.UpdatePlanner;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * A modification of the basic update planner. It dispatches each update
 * statement to the corresponding index planner.
 */
public class IndexUpdatePlanner implements UpdatePlanner {
	@Override
	public int executeInsert(InsertData data, Transaction tx) {
		String tblname = data.tableName();
		Plan p = new TablePlan(tblname, tx);
		
		// Construct a map from field names to values
		Map<String, Constant> fldValMap = new HashMap<String, Constant>();
		Iterator<Constant> valIter = data.vals().iterator();
		for (String fldname : data.fields()) {
			Constant val = valIter.next();
			fldValMap.put(fldname, val);
		}

		// Insert the record into the record file
		UpdateScan s = (UpdateScan) p.open();
		s.insert();
		for (Map.Entry<String, Constant> fldValPair : fldValMap.entrySet()) {
			s.setVal(fldValPair.getKey(), fldValPair.getValue());
		}
		RecordId rid = s.getRecordId();
		s.close();
		
		// Insert the record to all corresponding indexes
		Set<IndexInfo> indexes = new HashSet<IndexInfo>();
		for (String fldname : data.fields()) {
			List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblname, fldname, tx);
			indexes.addAll(iis);
		}
		
		for (IndexInfo ii : indexes) {
			Index idx = ii.open(tx);
			idx.insert(new SearchKey(ii.fieldNames(), fldValMap), rid, true);
			idx.close();
		}
		
		VanillaDb.statMgr().countRecordUpdates(data.tableName(), 1);
		return 1;
	}

	@Override
	public int executeDelete(DeleteData data, Transaction tx) {
		String tblName = data.tableName();
		TablePlan tp = new TablePlan(tblName, tx);
		Plan selectPlan = null;
		
		// Create a IndexSelectPlan if there is matching index in the predicate
		boolean usingIndex = false;
		selectPlan = IndexSelector.selectByBestMatchedIndex(tblName, tp, data.pred(), tx);
		if (selectPlan == null)
			selectPlan = new SelectPlan(tp, data.pred());
		else {
			selectPlan = new SelectPlan(selectPlan, data.pred());
			usingIndex = true;
		}
		
		// Retrieve all indexes
		List<IndexInfo> allIndexes = new LinkedList<IndexInfo>();
		Set<String> indexedFlds = VanillaDb.catalogMgr().getIndexedFields(tblName, tx);
		
		for (String indexedFld : indexedFlds) {
			List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblName, indexedFld, tx);
			allIndexes.addAll(iis);
		}
		
		// Open the scan
		UpdateScan s = (UpdateScan) selectPlan.open();
		int count = 0;
		s.beforeFirst();
		while (s.next()) {
			RecordId rid = s.getRecordId();
			
			// Delete the record from every index
			for (IndexInfo ii : allIndexes) {
				// Construct a key-value map
				Map<String, Constant> fldValMap = new HashMap<String, Constant>();
				for (String fldName : ii.fieldNames())
					fldValMap.put(fldName, s.getVal(fldName));
				SearchKey key = new SearchKey(ii.fieldNames(), fldValMap);
				
				// Delete from the index
				Index index = ii.open(tx);
				index.delete(key, rid, true);
				index.close();
			}
			
			// Delete the record from the record file
			s.delete();

			/*
			 * Re-open the index select scan to ensure the correctness of
			 * next(). E.g., index block before delete the current slot ^:
			 * [^5,5,6]. After the deletion: [^5,6]. When calling next() of
			 * index select scan, current slot pointer will move forward,
			 * [5,^6].
			 */
			if (usingIndex) {
				s.close();
				s = (UpdateScan) selectPlan.open();
				s.beforeFirst();
			}
			
			count++;
		}
		s.close();
		
		VanillaDb.statMgr().countRecordUpdates(data.tableName(), count);
		return count;
	}

	@Override
	public int executeModify(ModifyData data, Transaction tx) {
		String tblName = data.tableName();
		TablePlan tp = new TablePlan(tblName, tx);
		Plan selectPlan = null;
		
		// Create a IndexSelectPlan if there is matching index in the predicate
		selectPlan = IndexSelector.selectByBestMatchedIndex(tblName, tp, data.pred(), tx, data.targetFields());
		if (selectPlan == null)
			selectPlan = new SelectPlan(tp, data.pred());
		else
			selectPlan = new SelectPlan(selectPlan, data.pred());
		
		// Open all indexes associate with target fields
		Set<Index> modifiedIndexes = new HashSet<Index>();
		for (String fieldName : data.targetFields()) {
			List<IndexInfo> iiList = VanillaDb.catalogMgr().getIndexInfo(tblName, fieldName, tx);
			for (IndexInfo ii : iiList)
				modifiedIndexes.add(ii.open(tx));
		}
		
		// Open the scan
		UpdateScan s = (UpdateScan) selectPlan.open();
		s.beforeFirst();
		int count = 0;
		while (s.next()) {
			
			// Construct a mapping from field names to values
			Map<String, Constant> oldValMap = new HashMap<String, Constant>();
			Map<String, Constant> newValMap = new HashMap<String, Constant>();
			for (String fieldName : data.targetFields()) {
				Constant oldVal = s.getVal(fieldName);
				Constant newVal = data.newValue(fieldName).evaluate(s);
				
				oldValMap.put(fieldName, oldVal);
				newValMap.put(fieldName, newVal);
				s.setVal(fieldName, newVal);
			}
			
			RecordId rid = s.getRecordId();
			
			// Update the indexes
			for (Index index : modifiedIndexes) {
				// Construct a SearchKey for the old value
				Map<String, Constant> fldValMap = new HashMap<String, Constant>();
				for (String fldName : index.getIndexInfo().fieldNames()) {
					Constant oldVal = oldValMap.get(fldName);
					if (oldVal == null)
						oldVal = s.getVal(fldName);
					fldValMap.put(fldName, oldVal);
				}
				SearchKey oldKey = new SearchKey(index.getIndexInfo().fieldNames(), fldValMap);
				
				// Delete the old value from the index
				index.delete(oldKey, rid, true);
				
				// Construct a SearchKey for the new value
				fldValMap = new HashMap<String, Constant>();
				for (String fldName : index.getIndexInfo().fieldNames()) {
					Constant newVal = newValMap.get(fldName);
					if (newVal == null)
						newVal = s.getVal(fldName);
					fldValMap.put(fldName, newVal);
				}
				SearchKey newKey = new SearchKey(index.getIndexInfo().fieldNames(), fldValMap);
				
				// Insert the new value to the index
				index.insert(newKey, rid, true);
				
				index.close();
			}
			
			count++;
		}
		
		// Close opened indexes and the record file
		for (Index index : modifiedIndexes)
			index.close();
		s.close();
		
		VanillaDb.statMgr().countRecordUpdates(data.tableName(), count);
		return count;
	}

	@Override
	public int executeCreateTable(CreateTableData data, Transaction tx) {
		VanillaDb.catalogMgr().createTable(data.tableName(), data.newSchema(),
				tx);
		return 0;
	}

	@Override
	public int executeCreateView(CreateViewData data, Transaction tx) {
		VanillaDb.catalogMgr().createView(data.viewName(), data.viewDef(), tx);
		return 0;
	}

	@Override
	public int executeCreateIndex(CreateIndexData data, Transaction tx) {
		VanillaDb.catalogMgr().createIndex(data.indexName(), data.tableName(),
				data.fieldNames(), data.indexType(), tx);
		return 0;
	}

	@Override
	public int executeDropTable(DropTableData data, Transaction tx) {
		VanillaDb.catalogMgr().dropTable(data.tableName(), tx);
		return 0;
	}

	@Override
	public int executeDropView(DropViewData data, Transaction tx) {
		VanillaDb.catalogMgr().dropView(data.viewName(), tx);
		return 0;
	}

	@Override
	public int executeDropIndex(DropIndexData data, Transaction tx) {
		VanillaDb.catalogMgr().dropIndex(data.indexName(), tx);
		return 0;
	}
}
