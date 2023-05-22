package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;

public class LimitScan implements Scan {

    private Scan s;
    private int limit;

    public LimitScan(Scan s, int limit) {
        this.s = s;
        this.limit = limit;
    }

    @Override
    public Constant getVal(String fldName) {
        return s.getVal(fldName);
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        if (limit == 0) {
            return false;
        }
        limit--;
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
    
}
