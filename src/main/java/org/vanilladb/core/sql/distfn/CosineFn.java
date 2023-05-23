package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public class CosineFn implements DistanceFn {

    private VectorConstant query;
    private String fieldName;

    public CosineFn(String fld) {
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
        double querySum = 0;
        double vecSum = 0;
        for (int i = 0; i < vec.size(); i++) {
            sum += query.get(i) * vec.get(i);
            querySum += query.get(i) * query.get(i);
            vecSum += vec.get(i) * vec.get(i);
        }
        return sum / (Math.sqrt(querySum) * Math.sqrt(vecSum));
    }

    @Override
    public String fieldName() {
        return fieldName;
    }
    
}
