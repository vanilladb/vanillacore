package org.vanilladb.core.query.algebra.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.VectorEmbeddingData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.index.IndexSelectPlan;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.RecordComparator;

public class NearestNeighborPlan implements Plan {
    private Plan child;
    private PriorityQueue<Map<String, Constant>> pq;
    private int limit;
    private IndexInfo ii;
    // private RecordComparator comp;

    public NearestNeighborPlan(TablePlan tp, String tblName, VectorEmbeddingData queryVector, int limit, Transaction tx) {
        this.limit = limit;
        // if (limit == -1) {
        //     this.child = new SortPlan(p, distFn, tx);
        // } else {
        // //     this.child = p;
        // //     List<String> sortFlds = new ArrayList<String>();
        // //     sortFlds.add(distFn.fieldName());
            
        // //     List<Integer> sortDirs = new ArrayList<Integer>();
        // //     sortDirs.add(DIR_ASC);

        // //     assert sortFlds.size() == 1;
        // //     assert sortDirs.size() == 1;



        // //     this.comp = new RecordComparator(sortFlds, sortDirs, distFn);
        // //     this.pq = new PriorityQueue<>(limit, 
        // //         (Map<String, Constant> r1, Map<String, Constant> r2) -> 
        // //             -1 * comp.compare(r1.get(sortFlds.get(0)), r2.get(sortFlds.get(0)))
        // //         );
        //     // choose an index to use
        //     List<IndexInfo> ii = VanillaDb.catalogMgr().getIndexInfo(tblName, distFn.fieldName(), tx);
        //     assert ii.size() == 1; // Assume only one index is created for the embedding field
        //     IndexInfo idxInfo = ii.get(0);
        // }
        List<IndexInfo> idxInfos = VanillaDb.catalogMgr().getIndexInfo(tblName, queryVector.getEmbeddingField(), tx);
        if (idxInfos.size() > 0) {
            assert idxInfos.size() == 1;

            ii = idxInfos.get(0);
            Map<String, ConstantRange> searchRange = new HashMap<>();
            searchRange.put(queryVector.getEmbeddingField(), ConstantRange.newInstance(queryVector.getQueryVector()));

            this.child = new IndexSelectPlan(tp, ii, searchRange, tx);
            ((IndexSelectPlan) this.child).setEmbeddingField(queryVector);
        } else {
            this.child = new SortPlan(tp, queryVector, tx);
        }
    }

    class PriorityQueueScan implements Scan {
        private PriorityQueue<Map<String, Constant>> pq;
        private boolean isBeforeFirsted = false;

        public PriorityQueueScan(PriorityQueue<Map<String, Constant>> pq) {
            this.pq = pq;
        }

        @Override
        public Constant getVal(String fldName) {
            return pq.peek().get(fldName);
        }

        @Override
        public void beforeFirst() {
            this.isBeforeFirsted = true;
        }

        @Override
        public boolean next() {
            if (isBeforeFirsted) {
                isBeforeFirsted = false;
                return true;
            }
            pq.poll();
            return pq.size() > 0;
        }

        @Override
        public void close() {
            return;
        }

        @Override
        public boolean hasField(String fldName) {
            return pq.peek().containsKey(fldName);
        }
    }

    @Override
    public Scan open() {
        Scan s = child.open();
        if (limit > 0) { // if limit is provided
            s.beforeFirst();
            while (s.next()) {
                Map<String, Constant> fldVals = new HashMap<>();
                for (String fldName : child.schema().fields()) {
                    fldVals.put(fldName, s.getVal(fldName));
                }
                pq.add(fldVals);
                if (pq.size() > limit)
                    pq.poll();
            }
            s.close();
            return new NearestNeighborScan(new PriorityQueueScan(pq));
        }
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
