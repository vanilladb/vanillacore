package org.vanilladb.core.sql;

import java.sql.Types;

public class VectorType extends Type {
    private int size = -1;

    VectorType() {}
    VectorType(int size) {
        this.size = size;
    }
    @Override
    public int getSqlType() {
        return Types.ARRAY;
    }

    @Override
    public int getArgument() {
        return size;
    }

    @Override
    public boolean isFixedSize() {
        return true;
    }

    @Override
    public boolean isNumeric() {
        return false;
    }

    @Override
    public int maxSize() {
        return size * Float.BYTES;
    }

    @Override
    public Constant maxValue() {
        return null;
    }

    @Override
    public Constant minValue() {
        return null;
    }
}
