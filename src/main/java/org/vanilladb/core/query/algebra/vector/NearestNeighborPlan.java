package org.vanilladb.core.query.algebra.vector;

import java.util.List;
import java.util.ArrayList;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.VectorComparator;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class NearestNeighborPlan implements Plan {
    private Plan child;

    public NearestNeighborPlan(Plan p, VectorConstant query, Transaction tx) {
        List<String> sortFields = new ArrayList<String>();
        sortFields.add("emb"); // sort using the embedding field
        this.child = new SortPlan(p, sortFields, new VectorComparator(query), tx);
    }

    @Override
    public Scan open() {
        Scan s = child.open();
        return new NearestNeighborScan(s);
    }

    @Override
    public long blocksAccessed() {
        return child.blocksAccessed();
    }

    @Override
    public Schema schema() {
        return child.schema();
    }

    @Override
    public Histogram histogram() {
        return child.histogram();
    }

    @Override
    public long recordsOutput() {
        return child.recordsOutput();
    }
}
