package org.vanilladb.core.sql;

import java.util.Comparator;

// import jdk.incubator.vector.FloatVector;
// import jdk.incubator.vector.VectorSpecies;

public class VectorComparator implements Comparator<Record> {
    private String embFld;
    private VectorConstant query;
    // static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

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

    // private double euclideanDistance(VectorConstant vec) {
    //     int i = 0;
    //     int upperBound = SPECIES.loopBound(vec.length());
    //     float[] result = new float[vec.length()];
        
    //     for (; i < upperBound; i += SPECIES.length()) {
    //         var va = FloatVector.fromArray(SPECIES, vec.asJavaVal(), i);
    //         var vb = FloatVector.fromArray(SPECIES, query.asJavaVal(), i);
    //         var diff = va.sub(vb);
    //         var vc = diff.mul(diff);
    //         vc.intoArray(result, i);
    //     }
    //     for (; i < vec.length(); i++) {
    //         float diff = vec.get(i) - query.get(i);
    //         result[i] = diff * diff;
    //     }
    //     double sum = 0.0;
    //     for (int j = 0; j < result.length; j++) {
    //         sum += result[j];
    //     }
    //     return Math.sqrt(sum);
    // }

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
