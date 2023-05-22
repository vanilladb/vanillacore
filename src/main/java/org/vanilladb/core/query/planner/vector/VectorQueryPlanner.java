package org.vanilladb.core.query.planner.vector;

import org.vanilladb.core.query.algebra.ExplainPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.vector.ApproximateNearestNeighborPlan;
import org.vanilladb.core.query.algebra.vector.NearestNeighborPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.query.parse.VectorQueryData;
import org.vanilladb.core.query.planner.QueryPlanner;
import org.vanilladb.core.storage.tx.Transaction;


public class VectorQueryPlanner implements QueryPlanner {
    public Plan createPlan(VectorQueryData query, Transaction tx) {
        String tblName = query.getCollectionName();
        
        Plan p = new TablePlan(tblName, tx);

        if (query.isApproximate())
            // Implement approximate nearest neighbor search here
            p = new ApproximateNearestNeighborPlan(p, query.getVector(), tblName, query.getEmbeddingField(), tx);
        else
            p = new NearestNeighborPlan(p, query.getVector(), query.getEmbeddingField(), tx);

        if (query.isExplain()) {
            p = new ExplainPlan(p);
        }
        
        return p;
    }

    @Override
    public Plan createPlan(QueryData data, Transaction tx) {
        throw new UnsupportedOperationException();
    }
}
