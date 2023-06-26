package org.vanilladb.core.storage.index.ivf;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

public class IVFIndex extends Index {

    private static final String SCHEMA_CENTROID = "centroid", SCHEMA_RID_BLOCK = "block",
			SCHEMA_RID_ID = "id";

    private static final int NUM_CELLS;
    private static final int NUM_ITER;
    private static final int NUM_PROBE;

    static {
        NUM_CELLS = CoreProperties.getLoader().getPropertyAsInteger(
            IVFIndex.class.getName() + ".NUM_CELLS", 100);
        NUM_ITER = CoreProperties.getLoader().getPropertyAsInteger(
            IVFIndex.class.getName() + ".NUM_ITER", 10);
        NUM_PROBE = CoreProperties.getLoader().getPropertyAsInteger(
            IVFIndex.class.getName() + ".NUM_PROBE", 1);
    }

    private List<RecordFile> curBuckets = new ArrayList<>();
    private int bucketIdx;
    private boolean isBeforeFirsted = false;

    public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
    }

    private static int getNumRecords(TableScan s) {
        s.beforeFirst();
        int total = 0;
        while(s.next()) {
            total++;
        }
        return total;
    }

    private static VectorConstant[] initCentroids(TableScan s, int numElements, String embFld) {
        // Randomly choose centroids
        Random random = new Random(42);
        Set<Integer> ids = new LinkedHashSet<>();
        while (ids.size() < NUM_CELLS) {
            Integer next = random.nextInt(numElements);
            ids.add(next);
        }
        VectorConstant[] centroids = new VectorConstant[NUM_CELLS];
        s.beforeFirst();
        int idx = 0;
        int i = 0;

        while (s.next()) {
            if (ids.contains(i)) {
                VectorConstant v = (VectorConstant) s.getVal(embFld);
                centroids[idx++] = v;
            }
            i++;
        }

        return centroids;
    }

    private static VectorConstant[] updateCentroids(TableScan ts, VectorConstant[] centroids, DistanceFn distFn, String embField) {
        VectorConstant[] newCentroids = new VectorConstant[centroids.length];
        for (int i = 0; i < centroids.length; i++) {
            newCentroids[i] = VectorConstant.zeros(centroids[0].dimension());
        }

        int[] states = new int[centroids.length];

        ts.beforeFirst();

        while (ts.next()) {
            // Choose the closest centroid
            VectorConstant v = (VectorConstant) ts.getVal(embField);

            double min = Double.MAX_VALUE;
            int nearestCentroid = -1;

            for (int centroid = 0; centroid < centroids.length; centroid++) {
                double dist = distFn.distance(centroids[centroid], v);
                if (dist < min) {
                    min = dist;
                    nearestCentroid = centroid;
                }
            }
            states[nearestCentroid]++;

            // n++
            // avg = avg + (new_value - avg) / n
            VectorConstant tmp = (VectorConstant) v.sub(newCentroids[nearestCentroid]);
            tmp = (VectorConstant) tmp.div(new IntegerConstant(states[nearestCentroid]));
            newCentroids[nearestCentroid] = (VectorConstant) newCentroids[nearestCentroid].add(tmp);
        }
        return newCentroids;
    }
    
    public static void train(IndexInfo ii, TablePlan tp, String embFldName, DistanceFn distFn, Transaction tx) {
        TableScan s = (TableScan) tp.open();

        int numElements = getNumRecords(s);
        VectorConstant[] centroids = initCentroids(s, numElements, embFldName);

        // Optimize centroids
        for (int i = 0; i < NUM_ITER; i++) {
            centroids = updateCentroids(s, centroids, distFn, embFldName);
            // System.out.println(centroids[0]);
        }

        centroidToFile(centroids, ii, tx);
        dataToFile(s, ii, embFldName, centroids, distFn, tx);
    }

    private static void centroidToFile(VectorConstant[] centroids, IndexInfo ii, Transaction tx) {
        String centroidTblName = ii.indexName();
        Schema centroidSch = new Schema();
        centroidSch.addField(SCHEMA_CENTROID, Type.VECTOR(centroids[0].dimension()));
        TableInfo centroidTableInfo = new TableInfo(centroidTblName, centroidSch);

        RecordFile centroidFile = centroidTableInfo.open(tx, false);
        if (centroidFile.fileSize() == 0)
            RecordFile.formatFileHeader(centroidTableInfo.fileName(), tx);

        for (VectorConstant centroid : centroids) {
            centroidFile.insert();
            centroidFile.setVal(SCHEMA_CENTROID, centroid);
        }

        centroidFile.close();
    }

    private static void dataToFile(TableScan ts, IndexInfo ii, String embField, VectorConstant[] centroids, DistanceFn distFn, Transaction tx) {
        Schema dataPageSchema = new Schema();
        
        dataPageSchema.addField(SCHEMA_RID_BLOCK, BIGINT);
        dataPageSchema.addField(SCHEMA_RID_ID, INTEGER);

        ts.beforeFirst();
        while (ts.next()) {
            // Choose the closest centroid
            VectorConstant v = (VectorConstant) ts.getVal(embField);

            double min = Double.MAX_VALUE;
            int nearestCentroid = -1;

            for (int centroid = 0; centroid < centroids.length; centroid++) {
                double dist = distFn.distance(centroids[centroid], v);
                if (dist < min) {
                    min = dist;
                    nearestCentroid = centroid;
                }
            }

            // buckets[nearestCentroid]++;

            String tblName = ii.indexName() + nearestCentroid;
            TableInfo dataTableInfo = new TableInfo(tblName, dataPageSchema);
            RecordFile dataRecordFile = dataTableInfo.open(tx, false);

            if (dataRecordFile.fileSize() == 0) {
                RecordFile.formatFileHeader(dataTableInfo.fileName(), tx);
            }

            dataRecordFile.insert();
            RecordId rid = ts.getRecordId();
            dataRecordFile.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(rid.block().number()));
            dataRecordFile.setVal(SCHEMA_RID_ID, new IntegerConstant(rid.id()));

            dataRecordFile.close();
        }
    }

    @Override
    public void beforeFirst(SearchRange searchRange) {
        assert searchRange.isSingleValue(); // it is a single VectorConstant value

        String centroidTblName = ii.indexName();
        Schema centroidSch = new Schema();

        VectorConstant query = (VectorConstant) searchRange.asSearchKey().get(0);

        centroidSch.addField(SCHEMA_CENTROID, Type.VECTOR(query.dimension()));
        TableInfo centroidTableInfo = new TableInfo(centroidTblName, centroidSch);
        RecordFile centroidFile = centroidTableInfo.open(tx, false);

        centroidFile.beforeFirst();
        // double minDistance = Double.MAX_VALUE;
        // int nearestCentroid = -1;

        class CentroidDistance {
            public Integer id;
            public Double distance;
            public CentroidDistance(int id, double distance) {
                this.id = id; 
                this.distance = distance;
            }

            static int compare(CentroidDistance a, CentroidDistance b) {
                return Double.compare(a.distance, b.distance);
            }
        }

        PriorityQueue<CentroidDistance> pq = new PriorityQueue<>(NUM_PROBE+1, 
        (CentroidDistance a, CentroidDistance b) -> -CentroidDistance.compare(a, b));

        int idx = 0;
        while (centroidFile.next()) {
            VectorConstant c = (VectorConstant) centroidFile.getVal(SCHEMA_CENTROID);
            double dist = new EuclideanFn().distance(c, query);

            pq.add(new CentroidDistance(idx, dist));
            if (pq.size() > NUM_PROBE) {
                pq.poll();
            }

            // if (dist < minDistance) {
            //     minDistance = dist;
            //     nearestCentroid = idx;
            // }
            idx++;
        }

        Schema dataPageSchema = new Schema();

        dataPageSchema.addField(SCHEMA_RID_BLOCK, BIGINT);
        dataPageSchema.addField(SCHEMA_RID_ID, INTEGER);

        for (CentroidDistance cd : pq) {
            String tblName = ii.indexName() + cd.id;
            TableInfo dataTableInfo = new TableInfo(tblName, dataPageSchema);
            RecordFile bucket = dataTableInfo.open(tx, false);
            if (bucket.fileSize() == 0) {
                RecordFile.formatFileHeader(dataTableInfo.fileName(), tx);
            }
            bucket.beforeFirst();
            curBuckets.add(bucket);
        }

        // String tblName = ii.indexName() + nearestCentroid;
        // TableInfo dataTableInfo = new TableInfo(tblName, dataPageSchema);
        // curBucket = dataTableInfo.open(tx, false);

        // if (curBucket.fileSize() == 0) {
        //     RecordFile.formatFileHeader(dataTableInfo.fileName(), tx);
        // }
        // curBucket.beforeFirst();

        isBeforeFirsted = true;
        bucketIdx = 0;
    }

    @Override
    public boolean next() {
        if (!isBeforeFirsted)
            throw new IllegalStateException("You must call beforeFirst() before iterating index '" + ii.indexName() + "'");

        while (bucketIdx < curBuckets.size()) {
            while(curBuckets.get(bucketIdx).next()) {
                return true;
            }
            bucketIdx++;
        }

        return false;
    }

    @Override
    public RecordId getDataRecordId() {
        long blkNum = (Long) curBuckets.get(bucketIdx).getVal(SCHEMA_RID_BLOCK).asJavaVal();
        int id = (Integer) curBuckets.get(bucketIdx).getVal(SCHEMA_RID_ID).asJavaVal();
        return new RecordId(new BlockId(dataFileName, blkNum), id);
    }

    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        // Do nothing for now

        // beforeFirst(new SearchRange(key));

        // if (doLogicalLogging) {
        //     tx.recoveryMgr().logLogicalStart();
        // }

        // curBuckets.get(bucketIdx).insert();
        // curBuckets.get(bucketIdx).setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block().number()));
        // curBuckets.get(bucketIdx).setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));

        // if (doLogicalLogging) {
        //     tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key, dataRecordId.block().number(), dataRecordId.id());
        // }
    }

    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        beforeFirst(new SearchRange(key));
        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();

        while (next()) {
            if (getDataRecordId().equals(dataRecordId)) {
                curBuckets.get(bucketIdx).delete();
                return;
            }
        }

        if (doLogicalLogging)
            tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key, dataRecordId.block().number(), dataRecordId.id());
    }

    @Override
    public void close() {
        // TODO: Close the centroid list
        // if (curBucket != null)
        //     curBucket.close();
        for (RecordFile bucket : curBuckets) {
            bucket.close();
        }
    }

    private long fileSize(String fileName) {
        tx.concurrencyMgr().readFile(fileName);
        return VanillaDb.fileMgr().size(fileName);
    }

    @Override
    public void preLoadToMemory() {
        for (int i = 0; i < NUM_CELLS; i++) {
            String tblName = ii.indexName() + i + ".tbl";
            long size = fileSize(tblName);
            BlockId blk;
            for (int j = 0; j < size; j++) {
                blk = new BlockId(tblName, j);
                tx.bufferMgr().pin(blk);
            }
        }
    }
}
