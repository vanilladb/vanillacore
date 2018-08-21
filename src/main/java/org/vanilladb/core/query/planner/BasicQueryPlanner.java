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
package org.vanilladb.core.query.planner;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.query.algebra.ExplainPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProductPlan;
import org.vanilladb.core.query.algebra.ProjectPlan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The simplest, most naive query planner possible.
 */
public class BasicQueryPlanner implements QueryPlanner {

	/**
	 * Creates a query plan as follows. It first takes the product of all tables
	 * and views; it then selects on the predicate; and finally it projects on
	 * the field list.
	 */
	@Override
	public Plan createPlan(QueryData data, Transaction tx) {
		// Step 1: Create a plan for each mentioned table or view
		List<Plan> plans = new ArrayList<Plan>();
		for (String tblname : data.tables()) {
			String viewdef = VanillaDb.catalogMgr().getViewDef(tblname, tx);
			if (viewdef != null)
				plans.add(VanillaDb.newPlanner().createQueryPlan(viewdef, tx));
			else
				plans.add(new TablePlan(tblname, tx));
		}
		// Step 2: Create the product of all table plans
		Plan p = plans.remove(0);
		for (Plan nextplan : plans)
			p = new ProductPlan(p, nextplan);
		// Step 3: Add a selection plan for the predicate
		p = new SelectPlan(p, data.pred());
		// Step 4: Add a group-by plan if specified
		if (data.groupFields() != null) {
			p = new GroupByPlan(p, data.groupFields(), data.aggregationFn(), tx);
		}
		// Step 5: Project onto the specified fields
		p = new ProjectPlan(p, data.projectFields());
		// Step 6: Add a sort plan if specified
		if (data.sortFields() != null)
			p = new SortPlan(p, data.sortFields(), data.sortDirections(), tx);
		// Step 7: Add a explain plan if the query is explain statement
		if (data.isExplain())
			p = new ExplainPlan(p);
		return p;
	}
}
