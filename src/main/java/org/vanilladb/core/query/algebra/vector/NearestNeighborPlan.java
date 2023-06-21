package org.vanilladb.core.query.algebra.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.vanilladb.core.query.algebra.LimitPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.VectorEmbeddingData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.index.IndexSelectPlan;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;

public class NearestNeighborPlan implements Plan {
    private Plan child;
    private PriorityQueue<Map<String, Constant>> pq;
    private int limit;
    private IndexInfo ii;
    // private RecordComparator comp;

    public NearestNeighborPlan(TablePlan tp, String tblName, VectorEmbeddingData queryVector, int limit, Transaction tx) {
        this.limit = limit;

        Plan p = tp;
        
        List<IndexInfo> idxInfos = VanillaDb.catalogMgr().getIndexInfo(tblName, queryVector.getEmbeddingField(), tx);
        if (idxInfos.size() > 0) {
            assert idxInfos.size() == 1;

            ii = idxInfos.get(0);
            Map<String, ConstantRange> searchRange = new HashMap<>();
            searchRange.put(queryVector.getEmbeddingField(), ConstantRange.newInstance(queryVector.getQueryVector()));

            p = new IndexSelectPlan(tp, ii, searchRange, tx);
            ((IndexSelectPlan) p).setEmbeddingField(queryVector);
        } 

        p = new SortPlan(p, queryVector, tx);
        this.child = new LimitPlan(p, limit);
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
