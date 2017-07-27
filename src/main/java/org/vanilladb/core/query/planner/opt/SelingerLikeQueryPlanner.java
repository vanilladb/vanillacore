package org.vanilladb.core.query.planner.opt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.query.algebra.ExplainPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProjectPlan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.query.planner.QueryPlanner;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;

public class SelingerLikeQueryPlanner implements QueryPlanner{
	private AccessPath accessPath = new AccessPath();
	private Map<Integer, Plan> lookupTbl = new HashMap<Integer, Plan>();
	private ArrayList<TablePlanner> tablePlanners = new ArrayList<TablePlanner>();
	private Collection<Plan> views = new ArrayList<Plan>();

	@Override
	public Plan createPlan(QueryData data, Transaction tx) {
		// Step 1: Create a TablePlanner object for each mentioned table/view
		int i = 0;
		for (String tbl : data.tables()) {
			String viewdef = VanillaDb.catalogMgr().getViewDef(tbl, tx);
			if (viewdef != null)
				views.add(VanillaDb.newPlanner().createQueryPlan(viewdef, tx));
			else {
				TablePlanner tp = new TablePlanner(tbl, data.pred(), tx, i);
				tablePlanners.add(tp);
				i += 1;
			}
		}
		// step 2, 3: Use Selinger optimization to find join access path
		Plan trunk = getAccessPath(); 
		// Step 4: Add a group by plan if specified
		if (data.groupFields() != null)
			trunk = new GroupByPlan(trunk, data.groupFields(),
					data.aggregationFn(), tx);
		// Step 5. Project on the field names
		trunk = new ProjectPlan(trunk, data.projectFields());
		// Step 6: Add a sort plan if specified
		if (data.sortFields() != null)
			trunk = new SortPlan(trunk, data.sortFields(),
					data.sortDirections(), tx);
		// Step 7: Add a explain plan if the query is explain statement
		if (data.isExplain())
			trunk = new ExplainPlan(trunk);
		return trunk;
	}
	
	private Plan getAccessPath() {
		int finalKey = 0;
		
		// deal with view first
		while (!views.isEmpty()) {
			Plan p = getLowestView();
			
			if (p != null)
				accessPath.setTrunk(p);
		}
		
		// get access path for join by going through every combination
		for (int i = 1; i <= tablePlanners.size(); i++) {
			finalKey += Math.pow(2, tablePlanners.get(i-1).getIndexNum());
			getAllCombination(0, i, 0);
		}
		
		if (finalKey == 0)
			return accessPath.getTrunk();
		else
			return lookupTbl.get(finalKey);
	}
	
	private void getAllCombination(int layer, int length, int st) {
		// use DP and left deep to find cheapest plan
		if (layer == length) {
			TablePlanner rightOne = null;
			Plan bestPlan = null;
			int hashKey = 0;
			
			// if only one table, use select down strategy
			if (length == 1) {
				rightOne = accessPath.getTablePlanner(0);
				
				if (accessPath.getTrunk() != null) {
					Plan plan = rightOne.makeJoinPlan(accessPath.getTrunk());
					if (plan == null)
						plan = rightOne.makeProductPlan(accessPath.getTrunk());
				}
				else
					bestPlan = rightOne.makeSelectPlan();
			}
			else {
				// go left deep
				for (int i = 0; i < length; i++) {	
					rightOne = accessPath.getTablePlanner(i);
					hashKey = accessPath.hashCode() - (int) Math.pow(2, rightOne.getIndexNum());
					
					Plan plan = rightOne.makeJoinPlan(lookupTbl.get(hashKey));
					if (plan == null)
						plan = rightOne.makeProductPlan(lookupTbl.get(hashKey));
					
					if (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput())
						bestPlan = plan;
				}	
			}
			lookupTbl.put(accessPath.hashCode(), bestPlan);
			
			return;
		}

		// loop for each layer
		for (int i = st; i < tablePlanners.size(); i++) {
			// rest table size is not enough
			if (tablePlanners.size()-st < length-layer)
				return;
			
			// add tp into this layer
			accessPath.add(layer, tablePlanners.get(st));
			
			// pass new start position to next layer
			getAllCombination(layer+1, length, st+1);
			
			// every combination which contains tp[i] has already appeared
			accessPath.del(tablePlanners.get(st));
			st += 1;
		}
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
