package org.vanilladb.core.sql;

import java.util.Comparator;

public class VectorComparator implements Comparator<Record> {
    private String fld = "emb";
    private VectorConstant query;

    public VectorComparator(VectorConstant query) {
        this.query = query;
    }

    private double euclideanDistance(VectorConstant vec) {
        // TODO: SIMD
        assert vec.size() == query.size();
        double sum = 0.0;
        for (int i = 0; i < vec.size(); i++) {
            double diff = vec.get(i) - query.get(i);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    @Override
    public int compare(Record rec1, Record rec2) {
        // Does not support multi attribute sort for now
        VectorConstant vec1 = (VectorConstant) rec1.getVal(fld);
        VectorConstant vec2 = (VectorConstant) rec2.getVal(fld);

        double dist1 = euclideanDistance(vec1);
        double dist2 = euclideanDistance(vec2);

        return Double.compare(dist1, dist2);
    }
    
}
