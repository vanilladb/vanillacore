package org.vanilladb.core.storage.file;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.VECTOR;

import java.util.logging.Level;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.VectorConstant;

import java.util.logging.Logger;

public class FileVectorTest {
    private static Logger logger = Logger.getLogger(FileVectorTest.class.getName());

    private static FileMgr fm;

    @BeforeClass
    public static void init() {
        ServerInit.init(FileVectorTest.class);
        fm = VanillaDb.fileMgr();
        if (logger.isLoggable(Level.INFO))
            logger.info("BEGIN VECTOR FILE TEST");
    }

    @AfterClass
    public static void finish() {
        if (logger.isLoggable(Level.INFO))
            logger.info("FINISH VECTOR FILE TEST");
    }

    @Test
    public void testVectorRetrieval() {
        int vecSize = 512;
        Page p1 = new Page();
        VectorConstant v1 = new VectorConstant(vecSize);
        p1.setVal(0, v1);
        VectorConstant v1_retrieved = (VectorConstant) p1.getVal(0, VECTOR);
        assertTrue(v1.equals(v1_retrieved));
        assertFalse(v1.equals(new VectorConstant(vecSize)));
    }
}
