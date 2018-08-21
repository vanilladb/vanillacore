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

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The interface implemented by planners for the SQL select and explain
 * statements.
 */
public interface QueryPlanner {

	/**
	 * Creates a plan for the parsed query.
	 * 
	 * @param data
	 *            the parsed representation of the query
	 * @param tx
	 *            the calling transaction
	 * @return a plan for that query
	 */
	Plan createPlan(QueryData data, Transaction tx);
}
