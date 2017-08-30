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
package org.vanilladb.core.query.planner.opt;

import static org.vanilladb.core.storage.index.Index.IDX_BTREE;
import static org.vanilladb.core.storage.index.Index.IDX_HASH;

import java.util.Map;
import java.util.Set;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.index.IndexJoinPlan;
import org.vanilladb.core.query.algebra.index.IndexSelectPlan;
import org.vanilladb.core.query.algebra.multibuffer.MultiBufferProductPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * This class contains methods for planning a single table.
 */
class TablePlanner {
	private TablePlan tp;
	private Predicate pred;
	private Schema sch;
	private Map<String, IndexInfo> idxes;
	private Transaction tx;
	private int id;
	private int hashCode;

	/**
	 * Creates a new table planner. The specified predicate applies to the
	 * entire query. The table planner is responsible for determining which
	 * portion of the predicate is useful to the table, and when indexes are
	 * useful.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param pred
	 *            the query predicate
	 * @param tx
	 *            the calling transaction
	 */
	public TablePlanner(String tblName, Predicate pred, Transaction tx, int id) {
		this.pred = pred;
		this.tx = tx;
		this.id = id;
		this.hashCode = (int) Math.pow(2, id);
		tp = new TablePlan(tblName, tx);
		sch = tp.schema();
		idxes = VanillaDb.catalogMgr().getIndexInfo(tblName, tx);
	}
	
	// set an unique number to this table planner
	public int getId() {
		return id;
	}
	
	// use binary to represent the combination
	@ Override
	public int hashCode() {
		return hashCode;
	}

	/**
	 * Constructs a select plan for the table. The plan will use an indexselect,
	 * if possible.
	 * 
	 * @return a select plan for the table.
	 */
	public Plan makeSelectPlan() {
		Plan p = makeIndexSelectPlan();
		if (p == null)
			p = tp;
		return addSelectPredicate(p);
	}

	/**
	 * Constructs a join plan of the specified trunk and this table. The plan
	 * will use an indexjoin, if possible; otherwise a multi-buffer product
	 * join. The method returns null if no join is possible.
	 * 
	 * <p>
	 * The select predicate applicable to this table is pushed down below the
	 * join.
	 * </p>
	 * 
	 * @param trunk
	 *            the specified trunk of join
	 * @return a join plan of the trunk and this table
	 */
	public Plan makeJoinPlan(Plan trunk) {
		Schema trunkSch = trunk.schema();
		Predicate joinPred = pred.joinPredicate(sch, trunkSch);
		if (joinPred == null)
			return null;
		Plan p = makeIndexJoinPlan(trunk, trunkSch);
		if (p == null)
			p = makeProductJoinPlan(trunk, trunkSch);
		return p;
	}

	/**
	 * Constructs a product plan of the specified trunk and this table.
	 * 
	 * <p>
	 * The select predicate applicable to this table is pushed down below the
	 * product.
	 * </p>
	 * 
	 * @param trunk
	 *            the specified trunk of join
	 * @return a product plan of the trunk and this table
	 */
	public Plan makeProductPlan(Plan trunk) {
		Plan p = makeSelectPlan();
		return new MultiBufferProductPlan(trunk, p, tx);
	}

	/**
	 * Identify and construct an index select plan based on the predicate and
	 * indexes by examining the constants opposite to an indexed field in all
	 * terms of a predicate. Currently, fields and constants in a term cannot be
	 * moved across the term operator. Therefore this method may not identify
	 * all possible index selects. It is users' responsibility to issue queries
	 * that help the identification: e.g., "F < C", not "F - C < 0".
	 * 
	 */
	private Plan makeIndexSelectPlan() {
		for (String fld : idxes.keySet()) {
			ConstantRange searchRange = pred.constantRange(fld);
			if (searchRange == null)
				continue;
			IndexInfo ii = idxes.get(fld);
			if ((ii.indexType() == IDX_HASH && searchRange.isConstant())
					|| ii.indexType() == IDX_BTREE) {
				return new IndexSelectPlan(tp, ii, searchRange, tx);
			}
		}
		return null;
	}

	/**
	 * Identify and construct an index join plan based on the predicate and
	 * indexes by examining the fields opposite to an indexed field in all terms
	 * of a predicate. Currently, fields and constants in a term cannot be moved
	 * across the term operator. Therefore this method may not identify all
	 * possible index joins. It is users' responsibility to issue queries that
	 * help the identification: e.g., "F1 = F2", not "F1 - F2 = 0".
	 * 
	 */
	private Plan makeIndexJoinPlan(Plan trunk, Schema trunkSch) {
		for (String fld : idxes.keySet()) {
			Set<String> outerFlds = pred.joinFields(fld);
			if (outerFlds != null)
				for (String outerFld : outerFlds)
					if (trunkSch.hasField(outerFld)) {
						IndexInfo ii = idxes.get(fld);
						Plan p = new IndexJoinPlan(trunk, tp, ii, outerFld, tx);
						/*
						 * Ideally, a select plan for this table should be
						 * created before applying the join. However, since
						 * indexjoin require the rhs plan to be a TablePlan,
						 * select predicate can only be added after the join, .
						 * which makes indexselect for this table infeasible.
						 */
						p = addSelectPredicate(p);
						return addJoinPredicate(p, trunkSch);
					}
		}
		return null;
	}

	private Plan makeProductJoinPlan(Plan current, Schema currSch) {
		Plan p = makeProductPlan(current);
		return addJoinPredicate(p, currSch);
	}

	private Plan addSelectPredicate(Plan p) {
		Predicate selectPred = pred.selectPredicate(sch);
		if (selectPred != null)
			return new SelectPlan(p, selectPred);
		else
			return p;
	}

	private Plan addJoinPredicate(Plan p, Schema currSch) {
		Predicate joinPred = pred.joinPredicate(currSch, sch);
		if (joinPred != null)
			return new SelectPlan(p, joinPred);
		else
			return p;
	}
}
