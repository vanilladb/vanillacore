package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class EuclideanFn extends DistanceFn {
    
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        long sum = 0;
        var upperBound = SPECIES.loopBound(vec.dimension());

        int i = 0;
        for (; i < upperBound; i += SPECIES.length()) {
            var va = IntVector.fromArray(SPECIES, query.asJavaVal(), i);
            var vb = IntVector.fromArray(SPECIES, vec.asJavaVal(), i);

            var vdiff = va.sub(vb);
            var vsum = vdiff.mul(vdiff);
            sum += vsum.reduceLanes(VectorOperators.ADD);
        }

        for (; i < vec.dimension(); i++) {
            double diff = query.get(i) - vec.get(i);
            sum += diff * diff;
        }
        
        return Math.sqrt(sum);
    }
    
}
