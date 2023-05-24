package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public class CosineFn extends DistanceFn {

    public CosineFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {

        double sum = 0;
        double querySum = 0;
        double vecSum = 0;
        // WARNING: Don't use vec.size() here, it will return the number of bytes
        for (int i = 0; i < vec.dimension(); i++) { 
            sum += query.get(i) * vec.get(i);
            querySum += query.get(i) * query.get(i);
            vecSum += vec.get(i) * vec.get(i);
        }
        return sum / (Math.sqrt(querySum) * Math.sqrt(vecSum));
    }
}
