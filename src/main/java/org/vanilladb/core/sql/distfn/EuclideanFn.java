package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public class EuclideanFn extends DistanceFn {

    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        double sum = 0;
        for (int i = 0; i < vec.dimension(); i++) {
            double diff = query.get(i) - vec.get(i);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }
    
}
