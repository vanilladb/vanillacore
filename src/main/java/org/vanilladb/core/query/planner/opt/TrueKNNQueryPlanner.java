package org.vanilladb.core.query.planner.opt;

import java.util.ArrayList;
import java.util.Collection;

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

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 */
public class TrueKNNQueryPlanner implements QueryPlanner {
	private Collection<TablePlanner> tablePlanners = new ArrayList<TablePlanner>();
	private Collection<Plan> views = new ArrayList<Plan>();

	/**
	 * Creates an optimized left-deep query plan using the following heuristics.
	 * H1. Choose the smallest table (considering selection predicates) to be
	 * first in the join order. H2. Add the table to the join order which
	 * results in the smallest output records.
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
				TablePlanner tp = new TablePlanner(tbl, data.pred(), data.embeddingFields(), tx, id);
				tablePlanners.add(tp);
			}
			id += 1;
		}
		// Step 2: Choose the lowest-size plan to begin the trunk of join
		Plan trunk = getLowestSelectPlan();
		// Step 3: Repeatedly add a plan to the join trunk
		while (!tablePlanners.isEmpty() || !views.isEmpty()) {
			Plan p = getLowestJoinPlan(trunk);
			if (p != null)
				trunk = p;
			else
				// no applicable join
				trunk = getLowestProductPlan(trunk);
		}
		// Step 4: Add a group by plan if specified
		if (data.groupFields() != null)
			trunk = new GroupByPlan(trunk, data.groupFields(),
					data.aggregationFn(), tx);

		// Step 5: Add a sort plan if specified
		if (data.sortFields() != null)
			trunk = new SortPlan(trunk, data.sortFields(),
					data.sortDirections(), tx);

		// Step 6. Project on the field names
		trunk = new ProjectPlan(trunk, data.projectFields());

		// Step 7: Add a limit plan if specified
		if (data.limit() != -1)
			trunk = new LimitPlan(trunk, data.limit());
			
		// Step 8: Add a explain plan if the query is explain statement
		if (data.isExplain())
			trunk = new ExplainPlan(trunk);
		return trunk;
	}

	private Plan getLowestSelectPlan() {
		TablePlanner bestTp = null;
		Plan bestPlan = null;
		Plan bestView = null;
		for (TablePlanner tp : tablePlanners) {
			Plan plan = tp.makeSelectPlan();
			if (bestPlan == null
					|| plan.recordsOutput() < bestPlan.recordsOutput()) {
				bestTp = tp;
				bestPlan = plan;
			}
		}
		for (Plan v : views)
			if (bestPlan == null
					|| v.recordsOutput() < bestPlan.recordsOutput()) {
				bestPlan = v;
				bestView = v;
			}

		if (bestView != null)
			views.remove(bestView);
		else
			tablePlanners.remove(bestTp);
		return bestPlan;
	}

	private Plan getLowestJoinPlan(Plan current) {
		TablePlanner bestTp = null;
		Plan bestPlan = null;
		Plan bestView = null;
		for (TablePlanner tp : tablePlanners) {
			Plan plan = tp.makeJoinPlan(current);
			if (plan != null
					&& (bestPlan == null || plan.recordsOutput() < bestPlan
							.recordsOutput())) {
				bestTp = tp;
				bestPlan = plan;
			}
		}
		for (Plan v : views)
			if (bestPlan == null
					|| v.recordsOutput() < bestPlan.recordsOutput()) {
				bestPlan = v;
				bestView = v;
			}

		if (bestView != null)
			views.remove(bestView);
		else if (bestPlan != null)
			tablePlanners.remove(bestTp);
		return bestPlan;
	}

	private Plan getLowestProductPlan(Plan current) {
		TablePlanner bestTp = null;
		Plan bestPlan = null;
		Plan bestView = null;
		for (TablePlanner tp : tablePlanners) {
			Plan plan = tp.makeProductPlan(current);
			if (bestPlan == null
					|| plan.recordsOutput() < bestPlan.recordsOutput()) {
				bestTp = tp;
				bestPlan = plan;
			}
		}
		for (Plan v : views)
			if (bestPlan == null
					|| v.recordsOutput() < bestPlan.recordsOutput()) {
				bestPlan = v;
				bestView = v;
			}
		if (bestView != null)
			views.remove(bestView);
		else if (bestPlan != null)
			tablePlanners.remove(bestTp);
		return bestPlan;
	}
}
