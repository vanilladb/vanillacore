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
package org.vanilladb.core.query.planner.index;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.index.IndexSelectPlan;
import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.DropTableData;
import org.vanilladb.core.query.parse.DropViewData;
import org.vanilladb.core.query.parse.DropIndexData;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.planner.UpdatePlanner;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.storage.index.Index;
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

		// first, insert the record
		UpdateScan s = (UpdateScan) p.open();
		s.insert();
		RecordId rid = s.getRecordId();

		// then modify each field, inserting an index record if appropriate
		Map<String, IndexInfo> indexes = VanillaDb.catalogMgr().getIndexInfo(
				tblname, tx);

		Iterator<Constant> valIter = data.vals().iterator();
		for (String fldname : data.fields()) {
			Constant val = valIter.next();
			// first, insert into index
			IndexInfo ii = indexes.get(fldname);
			if (ii != null) {
				Index idx = ii.open(tx);
				idx.insert(val, rid, true);
				idx.close();
			}
			// insert into record file
			s.setVal(fldname, val);
		}
		s.close();
		VanillaDb.statMgr().countRecordUpdates(data.tableName(), 1);
		return 1;
	}

	@Override
	public int executeDelete(DeleteData data, Transaction tx) {
		String tblname = data.tableName();
		TablePlan tp = new TablePlan(tblname, tx);
		Plan selectPlan = null;
		Map<String, IndexInfo> indexInfoMap = VanillaDb.catalogMgr()
				.getIndexInfo(tblname, tx);
		String keyFld = null;
		// create a IndexSelectPlan if there is matching index in predicate
		for (String fld : indexInfoMap.keySet()) {
			ConstantRange cr = data.pred().constantRange(fld);
			if (cr != null) {
				IndexInfo ii = indexInfoMap.get(fld);
				selectPlan = new IndexSelectPlan(tp, ii, cr, tx);
				keyFld = fld;
				break;
			}
		}
		if (selectPlan == null)
			selectPlan = new SelectPlan(tp, data.pred());
		else
			selectPlan = new SelectPlan(selectPlan, data.pred());

		UpdateScan s = (UpdateScan) selectPlan.open();
		int count = 0;
		s.beforeFirst();
		while (s.next()) {
			RecordId rid = s.getRecordId();
			// delete the record from every index
			for (String fldname : indexInfoMap.keySet()) {
				Constant val = s.getVal(fldname);
				Index idx = indexInfoMap.get(fldname).open(tx);
				idx.delete(val, rid, true);
				idx.close();
			}
			s.delete();

			/*
			 * Re-open the index select scan to ensure the correctness of
			 * next(). E.g., index block before delete the current slot ^:
			 * [^5,5,6]. After the deletion: [^5,6]. When calling next() of
			 * index select scan, current slot pointer will move forward,
			 * [5,^6].
			 */
			if (keyFld != null) {
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
		String tblname = data.tableName();
		Map<String, IndexInfo> indexInfoMap = VanillaDb.catalogMgr()
				.getIndexInfo(tblname, tx);

		// open all indexes associate with target fields
		Collection<String> targetflds = data.targetFields();
		HashMap<String, Index> targetIdxMap = new HashMap<String, Index>();
		for (String fld : targetflds) {
			IndexInfo ii = indexInfoMap.get(fld);
			Index idx = (ii == null) ? null : ii.open(tx);
			if (idx != null)
				targetIdxMap.put(fld, idx);
		}

		TablePlan tp = new TablePlan(tblname, tx);
		Plan selectPlan = null;
		// create a IndexSelectPlan if there is matching index in predicate
		for (String fld : indexInfoMap.keySet()) {
			ConstantRange cr = data.pred().constantRange(fld);
			/*
			 * Don't select with index when the indexed fld is in target flds.
			 * Using the index select plan will result in endless loop. E.g.,
			 * "UPDATE test SET tid = 999 WHERE tid > 1".
			 */
			if (cr != null && !targetflds.contains(fld)) {
				IndexInfo ii = indexInfoMap.get(fld);
				selectPlan = new IndexSelectPlan(tp, ii, cr, tx);
				break;
			}
		}
		if (selectPlan == null)
			selectPlan = new SelectPlan(tp, data.pred());
		else
			selectPlan = new SelectPlan(selectPlan, data.pred());

		UpdateScan s = (UpdateScan) selectPlan.open();
		s.beforeFirst();
		int count = 0;
		while (s.next()) {
			Constant newval, oldval;
			for (String fld : targetflds) {
				newval = data.newValue(fld).evaluate(s);
				oldval = s.getVal(fld);

				// update the appropriate index, if it exists
				Index idx = targetIdxMap.get(fld);
				if (idx != null) {
					RecordId rid = s.getRecordId();
					idx.delete(oldval, rid, true);
					idx.insert(newval, rid, true);
				}
				s.setVal(fld, newval);
			}
			count++;
		}
		// close opened indexes
		for (String fld : targetflds) {
			Index idx = targetIdxMap.get(fld);
			if (idx != null)
				idx.close();
		}
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
				data.fieldName(), data.indexType(), tx);
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
