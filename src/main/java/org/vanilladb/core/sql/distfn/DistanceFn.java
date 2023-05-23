package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public interface DistanceFn {
    
    void setQueryVector(VectorConstant query);
    
    double distance(VectorConstant vec);

    String fieldName();
}
