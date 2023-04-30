package org.vanilladb.core.integration;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.planner.Planner;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.buffer.BufferConcurrencyTest;
import org.vanilladb.core.storage.tx.Transaction;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StarTest {

    private static Logger logger = Logger.getLogger(BufferConcurrencyTest.class.getName());

    @BeforeClass
    public static void init() {
        ServerInit.init(StarTest.class);
        loadTestbed();

        if (logger.isLoggable(Level.INFO))
            logger.info("BEGIN STAR TEST");
    }

    @AfterClass
    public static void finish() {
        if (logger.isLoggable(Level.INFO))
            logger.info("FINISH STAR TEST");
    }

    private static void loadTestbed() {
        ServerInit.loadTestbed();
        if (logger.isLoggable(Level.INFO))
            logger.info("TESTING DATA CREATED");
    }

    @Test
    public void testSingleTable() {
        Transaction tx = VanillaDb.txMgr().newTransaction(
                Connection.TRANSACTION_SERIALIZABLE, false);
        Planner planner = VanillaDb.newPlanner();

        String query = "SELECT * FROM course";
        Plan plan = planner.createQueryPlan(query, tx);
        Scan scan = plan.open();

        scan.beforeFirst();
        if (scan.next()) {
            Assert.assertEquals(scan.getVal("cid").asJavaVal(), 0);
            Assert.assertEquals(scan.getVal("title").asJavaVal(), "course0");
            Assert.assertEquals(scan.getVal("deptid").asJavaVal(), 0);
        }
    }

    @Test
    public void testMultiTable() {
        Transaction tx = VanillaDb.txMgr().newTransaction(
                Connection.TRANSACTION_SERIALIZABLE, false);
        Planner planner = VanillaDb.newPlanner();

        String query = "SELECT * FROM course, dept WHERE deptid=did";
        Plan plan = planner.createQueryPlan(query, tx);
        Scan scan = plan.open();

        scan.beforeFirst();

        if (scan.next()) {
            Assert.assertEquals(scan.getVal("cid").asJavaVal(), 0);
            Assert.assertEquals(scan.getVal("title").asJavaVal(), "course0");
            Assert.assertEquals(scan.getVal("deptid").asJavaVal(), 0);
            Assert.assertEquals(scan.getVal("did").asJavaVal(), 0);
            Assert.assertEquals(scan.getVal("dname").asJavaVal(), "dept0");
        }
    }
}
