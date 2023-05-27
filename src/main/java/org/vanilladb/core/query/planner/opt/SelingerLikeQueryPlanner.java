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
package org.vanilladb.core.query.planner.opt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.query.algebra.ExplainPlan;
import org.vanilladb.core.query.algebra.LimitPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProjectPlan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.query.planner.QueryPlanner;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;

public class SelingerLikeQueryPlanner implements QueryPlanner{
	private Map<Integer, AccessPath> lookupTbl = new HashMap<Integer, AccessPath>();
	private ArrayList<TablePlanner> tablePlanners = new ArrayList<TablePlanner>();
	private Collection<Plan> views = new ArrayList<Plan>();
	
	/**
	 * Creates a left-deep query plan using the Selinger optimization.
	 * Main idea is to find all permutation of table join order and to
	 * choose the cheapest plan. However, all permutation can be too 
	 * much for us to go through all. So we use DP and left-deep only 
	 * to optimize this process.
	 * 
	 */
	@Override
	public Plan createPlan(QueryData data, Transaction tx) {
		// Step 1: Create a TablePlanner object for each mentioned table/view
		int id = 0;
		for (String tbl : data.tables()) {
			String viewdef = VanillaDb.catalogMgr().getViewDef(tbl, tx);
			if (viewdef != null)
				views.add(VanillaDb.newPlanner().createQueryPlan(viewdef, tx));
			else {
				TablePlanner tp = new TablePlanner(tbl, data.pred(), tx, id);
				tablePlanners.add(tp);
				id += 1;
			}
		}
		// step 2: Use Selinger optimization to find join access path
		Plan trunk = getAccessPath(); 
		// Step 3: Add a group by plan if specified
		if (data.groupFields() != null)
			trunk = new GroupByPlan(trunk, data.groupFields(),
					data.aggregationFn(), tx);

		// Step 4: Add a sort plan if specified
		if (data.sortFields() != null)
			trunk = new SortPlan(trunk, data.sortFields(),
					data.sortDirections(), tx);

		// Step 5. Project on the field names
		trunk = new ProjectPlan(trunk, data.projectFields());

		// Step 6: Add a limit plan if specified
		if (data.limit() != -1)
			trunk = new LimitPlan(trunk, data.limit());
			
		// Step 7: Add a explain plan if the query is explain statement
		if (data.isExplain())
			trunk = new ExplainPlan(trunk);
		return trunk;
	}
	
	private Plan getAccessPath() {
		Plan viewTrunk = null;
		
		// deal with view first
		while (!views.isEmpty()) 
			viewTrunk = getLowestView();

		// use DP and left deep to find cheapest plan
		return getAllCombination(viewTrunk);
	}
	
	private Plan getAllCombination(Plan viewTrunk) {
		int finalKey = 0;

		// STEP 1: choose the best access path for each table
		for (TablePlanner tp : tablePlanners) {
			Plan bestPlan = null;
			if (viewTrunk != null) {
				bestPlan = tp.makeJoinPlan(viewTrunk);
				if (bestPlan == null)
					bestPlan = tp.makeProductPlan(viewTrunk);
			}
			else {
				bestPlan = tp.makeSelectPlan();
			}
			AccessPath ap = new AccessPath(tp, bestPlan);
			lookupTbl.put(ap.hashCode(), ap);

			// compute the final hash key
			finalKey += tp.hashCode();
		}

		// construct all combination layer by layer
		for (int layer = 2; layer <= tablePlanners.size(); layer++) {

			Set<Integer> keySet = new HashSet<Integer>(lookupTbl.keySet());
			
			// when layer >= 2, iterate all existing (layer-1) combination to join with all table planners to construct next layer
			for (Integer key: keySet) {
				AccessPath leftTrunk = lookupTbl.get(key);
				
				// go left deep
				for (TablePlanner rightOne: tablePlanners) {
					// only consider (layer-1) combination
					if (leftTrunk.getTblUsed().size() < layer-1)
						continue;
					
					// cannot join with table which combination already included
					if (leftTrunk.isUsed(rightOne.getId()))
						continue;
					
					// do join
					Plan bestPlan = rightOne.makeJoinPlan(leftTrunk.getPlan());
					if (bestPlan == null)
						bestPlan = rightOne.makeProductPlan(leftTrunk.getPlan());
					
					int newKey = leftTrunk.hashCode() + rightOne.hashCode();
					AccessPath ap = lookupTbl.get(newKey);
					
					// there is no access path contains this combination
					if (ap == null) {
						AccessPath newAp = new AccessPath(leftTrunk, rightOne, bestPlan);
						lookupTbl.put(newKey, newAp);
					} 
					else {
						// check whether new access path is better than previous
						if (bestPlan.recordsOutput() < ap.getCost()) {
							AccessPath newAp = new AccessPath(leftTrunk, rightOne, bestPlan);
							lookupTbl.put(newKey, newAp);
						}
					}	
				}
			}
		}
		
		if (finalKey == 0)
			return viewTrunk;
		else
			return lookupTbl.get(finalKey).getPlan();
	}
	
	private Plan getLowestView () {
		Plan bestView = null;
		
		for (Plan v : views)
			if (bestView == null || v.recordsOutput() < bestView.recordsOutput()) 
				bestView = v;

		if (bestView != null)
			views.remove(bestView);
		
		return bestView;
	}
}	