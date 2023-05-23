package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public interface DistanceFn {

    static void checkVectorLength(VectorConstant vec1, VectorConstant vec2) {
        if (vec1.dimension() != vec2.dimension())
            throw new IllegalArgumentException("Vector length does not match");
    }
    
    void setQueryVector(VectorConstant query);
    
    double distance(VectorConstant vec);

    String fieldName();
}
