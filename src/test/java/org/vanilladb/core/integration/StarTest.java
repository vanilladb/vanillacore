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
        // TODO: multi-table test
        Transaction tx = VanillaDb.txMgr().newTransaction(
                Connection.TRANSACTION_SERIALIZABLE, false);
        Planner planner = VanillaDb.newPlanner();

        planner.executeUpdate("CREATE table students (sid INT, name VARCHAR(20), major VARCHAR(2))", tx);
        planner.executeUpdate("CREATE table years (syid INT, year INT)", tx);
        planner.executeUpdate("INSERT INTO students (sid, name, major) VALUES (1, 'Alice', 'CS')", tx);
        planner.executeUpdate("INSERT INTO years (syid, year) VALUES (1, 3)", tx);

        tx.commit();
        if (logger.isLoggable(Level.INFO))
            logger.info("TESTING DATA CREATED");
    }

    @Test
    public void testSingleTable() {
        Transaction tx = VanillaDb.txMgr().newTransaction(
                Connection.TRANSACTION_SERIALIZABLE, false);
        Planner planner = VanillaDb.newPlanner();

        String query = "SELECT * FROM students";
        Plan plan = planner.createQueryPlan(query, tx);
        Scan scan = plan.open();

        scan.beforeFirst();
        if (scan.next()) {
            Assert.assertEquals(scan.getVal("sid").asJavaVal(), 1);
            Assert.assertEquals(scan.getVal("name").asJavaVal(), "Alice");
            Assert.assertEquals(scan.getVal("major").asJavaVal(), "CS");
        }
    }

    @Test
    public void testMultiTable() {
        Transaction tx = VanillaDb.txMgr().newTransaction(
                Connection.TRANSACTION_SERIALIZABLE, false);
        Planner planner = VanillaDb.newPlanner();

        String query = "SELECT * FROM students, years WHERE sid=syid";
        Plan plan = planner.createQueryPlan(query, tx);
        Scan scan = plan.open();

        scan.beforeFirst();

        if (scan.next()) {
            Assert.assertEquals(scan.getVal("sid").asJavaVal(), 1);
            Assert.assertEquals(scan.getVal("syid").asJavaVal(), 1);
            Assert.assertEquals(scan.getVal("name").asJavaVal(), "Alice");
            Assert.assertEquals(scan.getVal("major").asJavaVal(), "CS");
            Assert.assertEquals(scan.getVal("year").asJavaVal(), 3);
        }
    }
}
