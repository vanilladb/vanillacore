package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import java.util.List;

/**
 * 
 */
public class ApproximateNearestNeighborPlan implements Plan {
    private Plan child;
    private IndexInfo ii;
    private SearchKey query;
    private Transaction tx;

    public ApproximateNearestNeighborPlan(Plan p, VectorConstant query, String tblName, String embField, Transaction tx) {
        this.child = p;
        List<IndexInfo> indexInfos = VanillaDb.catalogMgr().getIndexInfo(tblName, embField, tx);
        System.out.println("DEBUG: indexInfos: " + indexInfos);
        this.ii = indexInfos.get(0);
        this.query = new SearchKey(query);
        this.tx = tx;
    }

    @Override
    public Scan open() {
        TableScan ts = (TableScan) child.open();
        Index idx = ii.open(tx);
        return new ApproximateNearestNeighborScan(idx, query, ts);
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
