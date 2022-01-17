package org.vanilladb.core.storage.index.btree;

import static org.junit.Assert.fail;

import java.lang.Thread.UncaughtExceptionHandler;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.planner.Planner;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

public class BTreeIndexConcurrentTest {
	private static Logger logger = Logger.getLogger(BTreeIndexConcurrentTest.class.getName());

	private static final Type ID_TYPE = Type.VARCHAR(33);
	private static final int THREAD_COUNT = 100;
	
	private static AtomicInteger nextInsertId = new AtomicInteger(1);
	private static Throwable exception = null;
	
	@BeforeClass
	public static void init() {
		ServerInit.init(BTreeIndexConcurrentTest.class);
		createIndex();
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN B-TREE CONCURRENCY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH B-TREE CONCURRENCY TEST");
	}
	
	private static void createIndex() {
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Planner planner = VanillaDb.newPlanner();
		
		// Create a table
		planner.executeUpdate("CREATE TABLE test (id VARCHAR(33), val INT)", tx);
		
		// Create a B-Tree index
		planner.executeUpdate("CREATE INDEX test_idx ON test (id) USING BTREE", tx);
		
		tx.commit();
		
		if (logger.isLoggable(Level.INFO))
			logger.info("TESTING DATA CREATED");
	}
	
	@Test
	public void testConcurrentInsert() {
		List<Thread> threads = new ArrayList<Thread>(100);
		
		for (int i = 0; i < THREAD_COUNT; i++) {
			Thread thread = new Thread(new Insertor());
			thread.setUncaughtExceptionHandler(new ExceptionCatcher());
			thread.start();
			threads.add(thread);
		}
		
		Thread r = new Thread(new Rollbacker());
		r.setUncaughtExceptionHandler(new ExceptionCatcher());
		r.start();
		
		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		try {
			r.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		if (exception != null) {
			exception.printStackTrace();
			fail("Found exception: " + exception);
		}
	}
	
	static class ExceptionCatcher implements UncaughtExceptionHandler {

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			exception = e;
		}
		
	}
	
	static class Insertor implements Runnable {

		@Override
		public void run() {
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			
			try {
				IndexInfo ii = VanillaDb.catalogMgr().getIndexInfo("test", "id", tx).get(0);
				Index idx = ii.open(tx);
				
				int insertId = nextInsertId.getAndIncrement();
				String idStr = String.format("%033d", insertId);
				Constant idCon = new VarcharConstant(idStr, ID_TYPE);
				RecordId fakeRid = new RecordId(new BlockId("test", insertId), 1);
				
				idx.insert(new SearchKey(idCon), fakeRid, true);
				
				tx.commit();
			} catch (LockAbortException l) {
				tx.rollback();
			}
		}
	}
	
	static class Rollbacker implements Runnable {

		@Override
		public void run() {
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			
			try {
				IndexInfo ii = VanillaDb.catalogMgr().getIndexInfo("test", "id", tx).get(0);
				Index idx = ii.open(tx);
				
				int insertId = nextInsertId.getAndIncrement();
				String idStr = String.format("%033d", insertId);
				Constant idCon = new VarcharConstant(idStr, ID_TYPE);
				RecordId fakeRid = new RecordId(new BlockId("test", insertId), 1);
				
				idx.insert(new SearchKey(idCon), fakeRid, true);
				
				tx.rollback();
			} catch (LockAbortException l) {
				tx.rollback();
			}
		}
		
	}
}
