package org.vanilladb.core.query.algebra.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static org.vanilladb.core.sql.RecordComparator.DIR_ASC;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.RecordComparator;

public class NearestNeighborPlan implements Plan {
    private Plan child;
    private PriorityQueue<Map<String, Constant>> pq;
    private int limit;
    private RecordComparator comp;

    public NearestNeighborPlan(Plan p, DistanceFn distFn, int limit, Transaction tx) {
        this.limit = limit;
        if (limit == -1) {
            this.child = new SortPlan(p, distFn, tx);
        } else {
            this.child = p;
            List<String> sortFlds = new ArrayList<String>();
            sortFlds.add(distFn.fieldName());
            
            List<Integer> sortDirs = new ArrayList<Integer>();
            sortDirs.add(DIR_ASC);

            assert sortFlds.size() == 1;
            assert sortDirs.size() == 1;

            this.comp = new RecordComparator(sortFlds, sortDirs, distFn);
            this.pq = new PriorityQueue<>(limit, 
            (Map<String, Constant> r1, Map<String, Constant> r2) -> 
                -1 * comp.compare(r1.get(sortFlds.get(0)), r2.get(sortFlds.get(0)))
            );
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
        if (limit > 0) {
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
