package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;

public class ApproximateNearestNeighborScan implements Scan {
    @Override
    public void beforeFirst() {

    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasField(String fldName) {
        return false;
    }

    @Override
    public Constant getVal(String fldName) {
        return null;
    }
}
