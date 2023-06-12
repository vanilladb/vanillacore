package org.vanilladb.core.storage.index.ivf;

import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.parse.VectorEmbeddingData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.VectorType;
import org.vanilladb.core.sql.distfn.DistanceFn;
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

    private static final int NUM_CELLS;
    private boolean isTrained = false; // TODO: Read from file

    static {
        NUM_CELLS = CoreProperties.getLoader().getPropertyAsInteger(
            IVFIndex.class.getName() + ".NUM_CELLS", 100);
    }

    public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
    }

    public void train(TablePlan tp, VectorEmbeddingData queryVec) {
        if (isTrained)
            return;

        Scan s = tp.open();
        s.beforeFirst();

        int numElements = 0;
        while (s.next()) {
            numElements++;
        }

        // Generate initial centroids
        Random random = new Random();
        Set<Integer> ids = new LinkedHashSet<>();
        while (ids.size() < NUM_CELLS) {
            Integer next = random.nextInt(numElements);
            ids.add(next);
        }

        VectorConstant[] centroids = new VectorConstant[NUM_CELLS];

        s.beforeFirst(); // TODO: is it okay to reuse a scan?
        int idx = 0;
        int i = 0;

        while (s.next()) {
            if (ids.contains(i)) {
                VectorConstant v = (VectorConstant) s.getVal(queryVec.getEmbeddingField());
                centroids[idx++] = v;
            }
            i++;
        }
        
        DistanceFn distFn = queryVec.getDistanceFn();

        String tblname = ii.indexName() + distFn;
        Schema centroidSch = new Schema();
        centroidSch.addField("emb", Type.VECTOR(centroids[0].dimension()));
        TableInfo ti = new TableInfo(tblname, centroidSch);
        RecordFile centroidFile = ti.open(tx, false);
        if (centroidFile.fileSize() == 0)
            RecordFile.formatFileHeader(ti.fileName(), tx);
        centroidFile.beforeFirst();
        for (VectorConstant centroid : centroids) {
            centroidFile.insert();
            centroidFile.setVal("emb", centroid);
        }
        centroidFile.close();

        // TODO: Create a file for each cluster
        for (int j = 0; j < centroids.length; j++) {
            String tblName = ii.indexName() + distFn + "-" + j;
        }

        s.beforeFirst();
        while (s.next()) {
            // Choose the closest centroid
            VectorConstant v = (VectorConstant) s.getVal(queryVec.getEmbeddingField());

            double min = Double.MAX_VALUE;
            int centroidId = -1;

            for (int j = 0; j < centroids.length; j++) {
                double dist = distFn.distance(centroids[j], v);
                if (dist < min) {
                    min = dist;
                    centroidId = j;
                }
            }


            // TODO: Insert to file
        }

        isTrained = true;
    }

    @Override
    public void beforeFirst(SearchRange searchRange) {
        assert searchRange.isSingleValue(); // it is a single VectorConstant value

    }

    @Override
    public boolean next() {
        throw new UnsupportedOperationException("Implement this");
    }

    @Override
    public RecordId getDataRecordId() {
        throw new UnsupportedOperationException("Implement this");
    }

    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        if (!isTrained) {
            // clusters have not yet been determined
            return;
        }
    }

    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        if (!isTrained)
            return;
    }

    @Override
    public void close() {
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
