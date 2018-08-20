package org.vanilladb.core.integration;

import java.sql.Connection;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
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
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.buffer.BufferConcurrencyTest;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionMgr;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

public class PhantomTest {
private static Logger logger = Logger.getLogger(BufferConcurrencyTest.class.getName());
	
	@BeforeClass
	public static void init() {
		ServerInit.init(PhantomTest.class);
		loadTestbed();
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN PHANTOM TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH PHANTOM TEST");
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
	
	@Test
	public void testPhantomRead() {
		CyclicBarrier barrier = new CyclicBarrier(3);
		TransactionMgr txMgr = VanillaDb.txMgr();
		Tx1Client tx1 = new Tx1Client(txMgr.newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true), barrier);
		Tx2Client tx2 = new Tx2Client(txMgr.newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false), barrier);
		
		// Start running
		tx1.start();
		tx2.start();
		
		try {
			// Phase 1: Tx 1 check max score
			barrier.await();
			// Phase 2: Tx 2 insert a record which alters the max score
			barrier.await();
			// Phase 3: Tx 1 check max score again
			barrier.await();
			
			// Wait for finishing
			tx1.join();
			tx2.join();
			
			// Ensure no exception happens
			Assert.assertTrue("Tx 1 throws an Exception", tx1.isSuccess());
			Assert.assertTrue("Tx 2 throws an Exception", tx2.isSuccess());
			
			// Check the result
			Assert.assertTrue("Phantom read happens", tx1.isSame());
			
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
		
		
	}
	
	private static class TxClient extends Thread {
		protected Transaction tx;
		private CyclicBarrier barrier;
		private boolean success;
		
		public TxClient(Transaction tx, CyclicBarrier barrier) {
			this.tx = tx;
			this.barrier = barrier;
		}

		@Override
		public void run() {
			try {
				barrier.await();
				phase1();
				barrier.await();
				phase2();
				barrier.await();
				phase3();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			success = true;
		}
		
		public void phase1() { };
		public void phase2() { };
		public void phase3() { };
		
		public boolean isSuccess() { return success; }
	}
	
	private static class Tx1Client extends TxClient {
		private Constant maxScore;
		private boolean isSame;

		public Tx1Client(Transaction tx, CyclicBarrier barrier) {
			super(tx, barrier);
		}
		
		@Override
		public void phase1() {
			maxScore = queryMaxScore();
			tx.endStatement();
		}
		
		@Override
		public void phase3() {
			Constant newMaxScore = queryMaxScore();
			isSame = maxScore.equals(newMaxScore);
			tx.commit();
		}
		
		public boolean isSame() {
			return isSame;
		}
		
		private Constant queryMaxScore() {
			Planner planner = VanillaDb.newPlanner();
			Plan p = planner.createQueryPlan("SELECT MAX(score) FROM test WHERE age = 20", tx);
			Scan s = p.open();
			s.beforeFirst();
			s.next();
			Constant value = s.getVal("maxofscore");
			s.close();
			return value;
		}
	}
	
	private static class Tx2Client extends TxClient {
		public Tx2Client(Transaction tx, CyclicBarrier barrier) {
			super(tx, barrier);
		}
		
		@Override
		public void phase2() {
			try {
				Planner planner = VanillaDb.newPlanner();
				planner.executeUpdate("UPDATE test SET age = 20 WHERE age = 23", tx);
			} catch (LockAbortException e) {
				// It is normal to be aborted here.
			}
			tx.commit();
		}
	}
}
