package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;

public class NearestNeighborScan implements Scan {

    Scan s;

    public NearestNeighborScan(Scan s) {
        this.s = s;
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        return s.next();
    }

    @Override
    public void close() {
        s.close();
    }

    @Override
    public boolean hasField(String fldName) {
        return s.hasField(fldName);
    }

    @Override
    public Constant getVal(String fldName) {
        return s.getVal(fldName);
    }
}
