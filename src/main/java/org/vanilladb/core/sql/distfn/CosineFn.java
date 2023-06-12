package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public class CosineFn extends DistanceFn {

    public CosineFn() {
    }

    @Override
    protected double calculateDistance(VectorConstant va, VectorConstant vb) {

        double sum = 0;
        double total1 = 0;
        double total2 = 0;
        // WARNING: Don't use vec.size() here, it will return the number of bytes
        for (int i = 0; i < va.dimension(); i++) { 
            sum += va.get(i) * vb.get(i);
            total1 += va.get(i) * va.get(i);
            total2 += vb.get(i) * vb.get(i);
        }
        return sum / (Math.sqrt(total1) * Math.sqrt(total2));
    }

    @Override
    public String toString() {
        return "cos";
    }
}
