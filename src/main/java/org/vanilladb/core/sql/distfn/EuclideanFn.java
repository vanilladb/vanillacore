package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class EuclideanFn extends DistanceFn {
    
    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    @Override
    protected double calculateDistance(VectorConstant v1, VectorConstant v2) {
        long sum = 0;
        var upperBound = SPECIES.loopBound(v1.dimension());

        int i = 0;
        for (; i < upperBound; i += SPECIES.length()) {
            var va = FloatVector.fromArray(SPECIES, v1.asJavaVal(), i);
            var vb = FloatVector.fromArray(SPECIES, v2.asJavaVal(), i);

            var vdiff = va.sub(vb);
            var vsum = vdiff.mul(vdiff);
            sum += vsum.reduceLanes(VectorOperators.ADD);
        }

        for (; i < v1.dimension(); i++) {
            double diff = v1.get(i) - v2.get(i);
            sum += diff * diff;
        }
        
        return Math.sqrt(sum);
    }

    @Override
    public String toString() {
        return "euc";
    }
    
}
