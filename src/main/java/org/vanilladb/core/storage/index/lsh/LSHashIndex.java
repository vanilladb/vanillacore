package org.vanilladb.core.storage.index.lsh;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
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

public class LSHashIndex extends Index {
    private static final int BANDS;
    private static final int BUCKETS;
    private RecordFile[] bucketFiles;

    static {
        BANDS = CoreProperties.getLoader().getPropertyAsInteger(
                LSHashIndex.class.getName() + ".BANDS", 4);
        BUCKETS = CoreProperties.getLoader().getPropertyAsInteger(
            LSHashIndex.class.getName() + ".NUM_BUCKETS", 99);
    }

    private boolean isBeforeFirsted;
    int curBand = 0;

    public LSHashIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
        bucketFiles = new RecordFile[BANDS];
    }

    private static Schema schema() {
        Schema sch = new Schema();
        sch.addField("id", INTEGER);
        sch.addField("block", BIGINT);
        sch.addField("slot", INTEGER);
        return sch;
    }

    @Override
    public void beforeFirst(SearchRange searchRange) {
        throw new UnsupportedOperationException("LSH index does not support range search");
    }

    public void beforeFirst(SearchKey searchKey) {
        close();
        // for id and vector
        assert searchKey.length() == 2;
        
        VectorConstant v = (VectorConstant) searchKey.get(1);
        
        assert v.length() % BANDS == 0;
        
        int[] hashValues = v.hashCode(BANDS, BUCKETS);

        for (int band = 0; band < BANDS; band++) {
            String tblName = ii.indexName() + "-" + band + "-" + hashValues[band] + ".tbl";
            TableInfo ti = new TableInfo(tblName, schema());
            bucketFiles[band] = ti.open(tx, false);

            if (bucketFiles[band].fileSize() == 0) {
                RecordFile.formatFileHeader(ti.fileName(), tx);
            }
            bucketFiles[band].beforeFirst();
        }
        isBeforeFirsted = true;
        curBand = 0;
    }

    @Override
    public void close() {
        for (int i = 0; i < BANDS; i++)
            if (bucketFiles[i] != null)
                bucketFiles[i].close();
    }

    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        throw new UnsupportedOperationException("NOT IMPLEMENTED");
    }

    @Override
    public RecordId getDataRecordId() {
        RecordFile rf = bucketFiles[curBand];
        long blkNum = (Long) rf.getVal("block").asJavaVal();
        int id = (Integer) rf.getVal("slot").asJavaVal();
        return new RecordId(new BlockId(ii.tableName(), blkNum), id);
    }

    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {

        beforeFirst(key);
        
        // log logical operation start
        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();

        for (int i = 0; i < BANDS; i++) {
            bucketFiles[i].insert();
            bucketFiles[i].setVal("id", key.get(0));
            bucketFiles[i].setVal("block", new BigIntConstant(dataRecordId.block().number()));
            bucketFiles[i].setVal("slot", new IntegerConstant(dataRecordId.id()));
        }
        
        // log logical operation end
        if (doLogicalLogging)
            tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
                    dataRecordId.block().number(), dataRecordId.id());
    }

    @Override
    public boolean next() {
        if (!isBeforeFirsted)
            throw new IllegalStateException("beforeFirst() is not called for LSH index");

        if (bucketFiles[curBand].next())
            return true;
        else {
            if (curBand == BANDS - 1)
                return false;
            else {
                curBand++;
                return next();
            }
        }
    }

    @Override
    public void preLoadToMemory() {
        throw new UnsupportedOperationException("Locality Hashing Index does not support pre-loading");
    }
}
