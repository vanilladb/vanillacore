package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public abstract class DistanceFn {

    public DistanceFn() {}

    public double distance(VectorConstant v1, VectorConstant v2) {
        // check vector dimension
        if (v1.dimension() != v2.dimension()) {
            throw new IllegalArgumentException("Vector length does not match");
        }
        return calculateDistance(v1, v2);
    }

    protected abstract double calculateDistance(VectorConstant v1, VectorConstant v2);

    @Override
    public abstract String toString();
}
