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

        Assert.assertTrue(v1.equals(v1_reconstructed));
        Assert.assertFalse(v1.equals(new VectorConstant(vecSize)));
        Assert.assertFalse(v1.equals(new VectorConstant(256)));
    }
}
