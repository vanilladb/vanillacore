package org.vanilladb.core.sql;

import org.junit.Assert;
import org.junit.Test;

public class VectorConstantTest {

    @Test
    public void testAsBytes() {
        int vecSize = 512;
        VectorConstant v1 = new VectorConstant(vecSize);

        byte[] vecAsBytes = v1.asBytes();
        VectorConstant v1_reconstructed = new VectorConstant(vecAsBytes);

        for (int i = 0; i < vecSize; i++) {
            Assert.assertTrue("Reconstructed vector does not match", v1.get(i) == v1_reconstructed.get(i));
        }
    }
}
