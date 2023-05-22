package org.vanilladb.core.integration;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.planner.Planner;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * For testing SQL only
 */
public class SQLTest {
    private static Logger logger = Logger.getLogger(SQLTest.class.getName());

    @BeforeClass
    public static void init() {
        ServerInit.init(SQLTest.class);
        ServerInit.loadTestbed();
        if (logger.isLoggable(Level.INFO)) {
            logger.info("TESTING DATA CREATED");
        }

        if (logger.isLoggable(Level.INFO))
            logger.info("BEGIN SQL TEST");
    }

    @AfterClass
    public static void finish() {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("FINISH SQL TEST");
        }
    }

    @Test
    public void testNoLimit() {
        Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, true);
        Planner planner = VanillaDb.newPlanner();
        
        int numStudents = 900;
        String sql = "SELECT sid, sname, majorid, gradyear FROM student";
        Plan p = planner.createQueryPlan(sql, tx);
        Scan s = p.open();

        int count = 0;
        s.beforeFirst();
        while (s.next()) {
            count++;
        }
        Assert.assertEquals(numStudents, count);
    }

    @Test
    public void testLimit() {
        Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, true);
        Planner planner = VanillaDb.newPlanner();
        
        int limit = 10;
        String sql = "SELECT sid, sname, majorid, gradyear FROM student LIMIT " + limit;
        Plan p = planner.createQueryPlan(sql, tx);
        Scan s = p.open();

        int count = 0;
        s.beforeFirst();
        while (s.next()) {
            count++;
        }
        Assert.assertEquals("The number of output records is not equal to the given LIMIT", 
                limit, count);
    }
}
