package org.vanilladb.core.query.planner.vector;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.query.parse.VectorQueryData;
import org.vanilladb.core.query.planner.QueryPlanner;
import org.vanilladb.core.storage.tx.Transaction;

public class VectorQueryPlanner implements QueryPlanner {
    public Plan createPlan(VectorQueryData query, Transaction tx) {
        // true knn or approximate knn
        // get collection name
        return null;
    }

    @Override
    public Plan createPlan(QueryData data, Transaction tx) {
        return null;
    }
}
