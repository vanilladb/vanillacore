package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.lsh.LSHashIndex;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.index.SearchKey;

public class ApproximateNearestNeighborScan implements Scan {

    private LSHashIndex idx;
    private SearchKey searchKey;
    private TableScan ts;

    public ApproximateNearestNeighborScan(Index idx, SearchKey searchKey, TableScan ts) {
        this.idx = (LSHashIndex) idx;
        this.searchKey = searchKey;
        this.ts = ts;
    }

    @Override
    public void beforeFirst() {
        idx.beforeFirst(searchKey);
    }

    @Override
    public boolean next() {
        boolean ok = idx.next();
        if (ok) {
            RecordId rid = idx.getDataRecordId();
            ts.moveToRecordId(rid);
        }
        return ok;
    }

    @Override
    public void close() {
        idx.close();
        ts.close();
    }

    @Override
    public boolean hasField(String fldName) {
        return ts.hasField(fldName);
    }

    @Override
    public Constant getVal(String fldName) {
        return ts.getVal(fldName);
    }
}
