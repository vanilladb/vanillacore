package org.vanilladb.core.server;

import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.integration.PhantomTest;
import org.vanilladb.core.query.planner.Planner;
import org.vanilladb.core.storage.buffer.BufferConcurrencyTest;
import org.vanilladb.core.storage.tx.Transaction;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ServerInitTest {
    private static Logger logger = Logger.getLogger(BufferConcurrencyTest.class.getName());
    
    
    @Test
    public void init() {
        ServerInit.init(ServerInitTest.class);
        loadTestbed();
        
        if (logger.isLoggable(Level.INFO))
            logger.info("BEGIN PHANTOM TEST");
    }
    
    private static void loadTestbed() {
        Transaction tx = VanillaDb.txMgr().newTransaction(
                Connection.TRANSACTION_SERIALIZABLE, false);
        Planner planner = VanillaDb.newPlanner();
        
        // Create a table
        planner.executeUpdate("CREATE TABLE test (age INT, score INT)", tx);
        
        // Create a B-Tree index
        planner.executeUpdate("CREATE INDEX age_idx ON test (age) USING BTREE", tx);
        
        // Insert a few record
        planner.executeUpdate("INSERT INTO test (age, score) VALUES (18, 80)", tx);
        planner.executeUpdate("INSERT INTO test (age, score) VALUES (20, 65)", tx);
        planner.executeUpdate("INSERT INTO test (age, score) VALUES (23, 95)", tx);
        
        tx.commit();
        
        if (logger.isLoggable(Level.INFO))
            logger.info("TESTING DATA CREATED");
    }
}
