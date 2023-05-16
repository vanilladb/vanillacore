package org.vanilladb.core.sql;

import java.util.Comparator;

public class VectorComparator implements Comparator<Record> {
    private String embFld;
    private VectorConstant query;

    public VectorComparator(VectorConstant query, String embFld) {
        this.query = query;
        this.embFld = embFld;
    }

    private double euclideanDistance(VectorConstant vec) {
        // TODO: SIMD
        assert vec.length() == query.length();
        double sum = 0.0;
        for (int i = 0; i < vec.length(); i++) {
            double diff = vec.get(i) - query.get(i);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    @Override
    public int compare(Record rec1, Record rec2) {
        // Does not support multi attribute sort for now
        VectorConstant vec1 = (VectorConstant) rec1.getVal(embFld);
        VectorConstant vec2 = (VectorConstant) rec2.getVal(embFld);

        double dist1 = euclideanDistance(vec1);
        double dist2 = euclideanDistance(vec2);

        return Double.compare(dist1, dist2);
    }

    public int compare(VectorConstant v1, VectorConstant v2) {
        // Does not support multi attribute sort for now

        double dist1 = euclideanDistance(v1);
        double dist2 = euclideanDistance(v2);

        return Double.compare(dist1, dist2);
    }
    
}
