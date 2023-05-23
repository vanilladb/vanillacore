package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public class EuclideanFn implements DistanceFn {

    private VectorConstant query;
    private String fieldName;

    public EuclideanFn(String fld) {
        this.fieldName = fld;
    }

    @Override
    public void setQueryVector(VectorConstant query) {
        this.query = query;
    }

    @Override
    public double distance(VectorConstant vec) {
        DistanceFn.checkVectorLength(vec, query);

        double sum = 0;
        for (int i = 0; i < vec.dimension(); i++) {
            double diff = query.get(i) - vec.get(i);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    @Override
    public String fieldName() {
        return this.fieldName;
    }
    
}
