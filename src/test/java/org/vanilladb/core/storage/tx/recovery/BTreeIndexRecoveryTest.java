package org.vanilladb.core.storage.tx.recovery;

import java.sql.Connection;
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

public class BTreeIndexRecoveryTest {
	private static Logger logger = Logger.getLogger(BTreeIndexRecoveryTest.class.getName());

	private static final Type ID_TYPE = Type.VARCHAR(33);
	
	@BeforeClass
	public static void init() {
		ServerInit.init(BTreeIndexRecoveryTest.class);
		createIndex();
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN B-TREE RECOVERY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH B-TREE RECOVERY TEST");
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
	public void testVarcharIdRollback() {
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		
		IndexInfo ii = VanillaDb.catalogMgr().getIndexInfo("test", "id", tx).get(0);
		Index idx = ii.open(tx);
		
		int insertId = 1;
		String idStr = String.format("%033d", insertId);
		Constant idCon = new VarcharConstant(idStr, ID_TYPE);
		RecordId fakeRid = new RecordId(new BlockId("test", insertId), 1);
		
		idx.insert(new SearchKey(idCon), fakeRid, true);
		
		tx.rollback();
	}

}
