package org.vanilladb.core.sql;

import java.sql.Types;

public class VectorType extends Type {

    @Override
    public int getSqlType() {
        return Types.ARRAY;
    }

    @Override
    public int getArgument() {
        return -1;
    }

    @Override
    public boolean isFixedSize() {
        return false;
    }

    @Override
    public boolean isNumeric() {
        return false;
    }

    @Override
    public int maxSize() {
        return 0;
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
