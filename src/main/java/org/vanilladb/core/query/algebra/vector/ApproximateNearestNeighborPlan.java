package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

/**
 * 
 */
public class ApproximateNearestNeighborPlan implements Plan {
    private Plan child;

    public ApproximateNearestNeighborPlan(Plan p) {
        this.child = p;
    }

    @Override
    public Scan open() {
        /*
         * for each record
         *   if dist(query, record) < k_max_distance
         *     arg_max()
         */
        return new ApproximateNearestNeighborScan();
    }

    @Override
    public long blocksAccessed() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Schema schema() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Histogram histogram() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long recordsOutput() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
