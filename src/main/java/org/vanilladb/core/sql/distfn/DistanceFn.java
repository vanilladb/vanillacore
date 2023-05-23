package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public interface DistanceFn {

    static void checkVectorLength(VectorConstant vec1, VectorConstant vec2) {
        if (vec1.length() != vec2.length())
            throw new IllegalArgumentException("Vector length does not match");
    }
    
    void setQueryVector(VectorConstant query);
    
    double distance(VectorConstant vec);

    String fieldName();
}
