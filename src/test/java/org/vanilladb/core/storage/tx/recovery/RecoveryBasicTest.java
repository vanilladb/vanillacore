/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.vanilladb.core.storage.tx.recovery;

import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.index.Index.IDX_BTREE;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.BarrierStartRunner;

public class RecoveryBasicTest {
	private static Logger logger = Logger.getLogger(RecoveryBasicTest.class.getName());
	
	private static String fileName = "recoverybasictest.0";
	private static String dataTableName = "recoverybasictest";
	private static CatalogMgr md;

	private static BlockId blk;

	@BeforeClass
	public static void init() {
		ServerInit.init(RecoveryBasicTest.class);
		
		blk = new BlockId(fileName, 12);
		md = VanillaDb.catalogMgr();
		Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);

		Schema schema = new Schema();
		schema.addField("cid", INTEGER);
		schema.addField("title", VARCHAR(20));
		schema.addField("majorid", BIGINT);
		md.createTable(dataTableName, schema, tx);
		md.createIndex("index_cid", dataTableName, "cid", IDX_BTREE, tx);

		tx.commit();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN RECOVERY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH RECOVERY TEST");
	}

	@Before
	public void setup() {

		// reset initial values in the block
		// Dummy txNum
		Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		long txNum = tx.getTransactionNumber();
		Buffer buff = tx.bufferMgr().pin(blk);
		buff.setVal(4, new IntegerConstant(9876), txNum, null);
		buff.setVal(20, new VarcharConstant("abcdefg"), txNum, null);
		buff.setVal(40, new VarcharConstant("hijk"), txNum, null);
		buff.setVal(104, new IntegerConstant(9999), txNum, null);
		buff.setVal(120, new VarcharConstant("gfedcba"), txNum, null);
		buff.setVal(140, new VarcharConstant("kjih"), txNum, null);
		buff.setVal(204, new IntegerConstant(1415), txNum, null);
		buff.setVal(220, new VarcharConstant("pifo"), txNum, null);
		buff.setVal(240, new VarcharConstant("urth"), txNum, null);
		buff.setVal(304, new IntegerConstant(9265), txNum, null);
		buff.setVal(320, new VarcharConstant("piei"), txNum, null);
		buff.setVal(340, new VarcharConstant("ghth"), txNum, null);
		buff.setVal(404, new IntegerConstant(0), txNum, null);
		buff.setVal(420, new VarcharConstant("aaaa"), txNum, null);
		buff.setVal(440, new VarcharConstant("AAAA"), txNum, null);
		buff.setVal(504, new IntegerConstant(0), txNum, null);
		buff.setVal(520, new VarcharConstant("aaaa"), txNum, null);
		buff.setVal(540, new VarcharConstant("AAAA"), txNum, null);
		tx.bufferMgr().flushAll(txNum);
		tx.bufferMgr().unpin(buff);
		tx.commit();
	}
	@Test
	public void testRollback() {
		// log and make changes to the block's values

		LinkedList<BlockId> blklist = new LinkedList<BlockId>();
		blklist.add(blk);

		Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm = tx.recoveryMgr();
		long txNum = tx.getTransactionNumber();
		BufferMgr bm = tx.bufferMgr();
		Buffer buff = bm.pin(blk);
		LogSeqNum lsn = rm.logSetVal(buff, 4, new IntegerConstant(1234));
		buff.setVal(4, new IntegerConstant(1234), txNum, lsn);
		lsn = rm.logSetVal(buff, 20, new VarcharConstant("xyz"));
		buff.setVal(20, new VarcharConstant("xyz"), txNum, lsn);

		bm.unpin(buff);
		bm.flushAll(txNum);

		// verify that the changes got made
		buff = bm.pin(blk);
		assertTrue("*****RecoveryTest: rollback changes not made",
				buff.getVal(4, INTEGER).equals(new IntegerConstant(1234))
						&& ((String) buff.getVal(20, VARCHAR).asJavaVal()).equals("xyz"));
		bm.unpin(buff);

		rm.onTxRollback(tx);

		// verify that they got rolled back
		buff = bm.pin(blk);
		int ti = (Integer) buff.getVal(4, INTEGER).asJavaVal();
		String ts = (String) buff.getVal(20, VARCHAR).asJavaVal();
		assertTrue("*****RecoveryTest: bad rollback", ti == 9876 && ts.equals("abcdefg"));
		bm.unpin(buff);
	}
	@Test
	public void testRecover() {

		CyclicBarrier startBarrier = new CyclicBarrier(4);
		CyclicBarrier endBarrier = new CyclicBarrier(4);

		// Tx1 Commit after checking
		new SetValTx(startBarrier, endBarrier, blk, 104, new IntegerConstant(1234), 0, 0).start();
		// Tx2 Commit before checking
		new SetValTx(startBarrier, endBarrier, blk, 120, new VarcharConstant("xyz"), 2, 0).start();
		// Tx3 Rollback before checking
		new SetValTx(startBarrier, endBarrier, blk, 140, new VarcharConstant("rst"), 0, 0).start();

		// Wait for setValue running
		try {
			startBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		// verify that the changes got made
		Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		Buffer buff = tx.bufferMgr().pin(blk);
		assertTrue("*****RecoveryTest: recovery changes not made",
				buff.getVal(104, INTEGER).equals(new IntegerConstant(1234))
						&& ((String) buff.getVal(120, VARCHAR).asJavaVal()).equals("xyz")
						&& ((String) buff.getVal(140, VARCHAR).asJavaVal()).equals("rst"));
		tx.bufferMgr().unpin(buff);

		// Wait for checking
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		Transaction recoveryTx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr.recover(recoveryTx);
		// verify that tx1 and tx3 got rolled back
		buff = recoveryTx.bufferMgr().pin(blk);
		int ti = (Integer) buff.getVal(104, INTEGER).asJavaVal();
		String ts = (String) buff.getVal(120, VARCHAR).asJavaVal();
		String ts2 = (String) buff.getVal(140, VARCHAR).asJavaVal();
		assertTrue("*****RecoveryTest: bad recovery", ti == 9999 && ts.equals("xyz") && ts2.equals("kjih"));
		recoveryTx.bufferMgr().unpin(buff);
	}
	@Test
	public void testCrashingDuringRecovery() {

		Transaction tx1 = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		Transaction tx2 = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		Transaction tx3 = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		Long txNum1 = tx1.getTransactionNumber();
		Long txNum2 = tx2.getTransactionNumber();
		Long txNum3 = tx3.getTransactionNumber();

		Buffer buff = tx1.bufferMgr().pin(blk);

		LogSeqNum lsn1 = tx1.recoveryMgr().logSetVal(buff, 404, new IntegerConstant(1111));
		buff.setVal(404, new IntegerConstant(1111), txNum1, lsn1);

		LogSeqNum lsn2 = tx2.recoveryMgr().logSetVal(buff, 420, new VarcharConstant("bbbb"));
		buff.setVal(420, new VarcharConstant("bbbb"), txNum2, lsn2);

		LogSeqNum lsn3 = tx3.recoveryMgr().logSetVal(buff, 440, new VarcharConstant("BBBB"));
		buff.setVal(440, new VarcharConstant("BBBB"), txNum3, lsn3);

		lsn1 = tx1.recoveryMgr().logSetVal(buff, 404, new IntegerConstant(2222));
		buff.setVal(404, new IntegerConstant(2222), txNum1, lsn1);

		lsn2 = tx2.recoveryMgr().logSetVal(buff, 420, new VarcharConstant("cccc"));
		buff.setVal(420, new VarcharConstant("cccc"), txNum2, lsn2);

		lsn3 = tx3.recoveryMgr().logSetVal(buff, 440, new VarcharConstant("CCCC"));
		buff.setVal(440, new VarcharConstant("CCCC"), txNum3, lsn3);

		tx1.bufferMgr().unpin(buff);

		tx3.commit();

		// verify that the changes got made
		Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);

		buff = tx.bufferMgr().pin(blk);
		assertTrue("*****RecoverFormCrashing: changes not made",
				buff.getVal(404, INTEGER).equals(new IntegerConstant(2222))
						&& ((String) buff.getVal(420, VARCHAR).asJavaVal()).equals("cccc")
						&& ((String) buff.getVal(440, VARCHAR).asJavaVal()).equals("CCCC"));
		tx.bufferMgr().unpin(buff);

		// Do partial recovery to simulate crash druing recovery;
		Transaction partRecoveryTx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr.partialRecover(partRecoveryTx, 5);

		// Do total recovery again
		Transaction recoveryTx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr.recover(recoveryTx);

		// verify that tx1 and tx2 got rolled back
		buff = recoveryTx.bufferMgr().pin(blk);
		int ti = (Integer) buff.getVal(404, INTEGER).asJavaVal();
		String ts = (String) buff.getVal(420, VARCHAR).asJavaVal();
		String ts2 = (String) buff.getVal(440, VARCHAR).asJavaVal();
		assertTrue("*****CrashingDuringRecoveryTest: bad recovery", ti == 0 && ts.equals("aaaa") && ts2.equals("CCCC"));

		int clrCount = 0;
		ReversibleIterator<LogRecord> iter = new LogRecordIterator();
		for (int i = 0; i < 8; i++) {
			LogRecord rec = iter.next();
			if (rec instanceof CompesationLogRecord)
				clrCount++;

		}
		assertTrue("*****CrashingDuringRecoveryTest: UndoNext failure", clrCount >= 4);
		recoveryTx.bufferMgr().unpin(buff);

	}

	@Test
	public void testCrashingDuringRollBack() {

		Transaction tx1 = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		Transaction tx2 = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		Transaction tx3 = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		Long txNum1 = tx1.getTransactionNumber();
		Long txNum2 = tx2.getTransactionNumber();
		Long txNum3 = tx3.getTransactionNumber();

		Buffer buff = tx1.bufferMgr().pin(blk);

		LogSeqNum lsn1 = tx1.recoveryMgr().logSetVal(buff, 504, new IntegerConstant(1111));
		buff.setVal(504, new IntegerConstant(1111), txNum1, lsn1);

		LogSeqNum lsn2 = tx2.recoveryMgr().logSetVal(buff, 520, new VarcharConstant("bbbb"));
		buff.setVal(520, new VarcharConstant("bbbb"), txNum2, lsn2);

		LogSeqNum lsn3 = tx3.recoveryMgr().logSetVal(buff, 540, new VarcharConstant("BBBB"));
		buff.setVal(540, new VarcharConstant("BBBB"), txNum3, lsn3);

		lsn1 = tx1.recoveryMgr().logSetVal(buff, 504, new IntegerConstant(2222));
		buff.setVal(504, new IntegerConstant(2222), txNum1, lsn1);

		lsn2 = tx2.recoveryMgr().logSetVal(buff, 520, new VarcharConstant("cccc"));
		buff.setVal(520, new VarcharConstant("cccc"), txNum2, lsn2);

		lsn3 = tx3.recoveryMgr().logSetVal(buff, 540, new VarcharConstant("CCCC"));
		buff.setVal(540, new VarcharConstant("CCCC"), txNum3, lsn3);

		tx1.bufferMgr().unpin(buff);

		
		tx3.commit();

		// verify that the changes got made
		Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);

		buff = tx.bufferMgr().pin(blk);
		assertTrue("*****CrashingDuringRollBackTest: changes not made",
				buff.getVal(504, INTEGER).equals(new IntegerConstant(2222))
						&& ((String) buff.getVal(520, VARCHAR).asJavaVal()).equals("cccc"));
		tx.bufferMgr().unpin(buff);

		// Do partial recovery to simulate crash druing recovery;

		RecoveryMgr.partialRollback(tx1, 5);
	
		RecoveryMgr.partialRollback(tx2, 5);
		
		// Do total recovery again
		Transaction recoveryTx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr.recover(recoveryTx);

		// verify that tx1 and tx2 got rolled back
		buff = recoveryTx.bufferMgr().pin(blk);
		int ti = (Integer) buff.getVal(504, INTEGER).asJavaVal();
		String ts = (String) buff.getVal(520, VARCHAR).asJavaVal();
		String ts2 = (String) buff.getVal(540, VARCHAR).asJavaVal();
		assertTrue("*****CrashingDuringRollBackTest: bad rollback", ti == 0 && ts.equals("aaaa") && ts2.equals("CCCC"));
		
		int clrCount = 0;
		ReversibleIterator<LogRecord> iter = new LogRecordIterator();
		for(int i = 0 ; i < 8 ; i ++){
			LogRecord rec = iter.next();
			if(rec instanceof CompesationLogRecord)
				clrCount++;

		}
		assertTrue("*****CrashingDuringRollBackTest: UndoNext failure", clrCount>=4 );
		recoveryTx.bufferMgr().unpin(buff);
		
		

	}
	@Test
	public void testCheckpoint() {

		CyclicBarrier startBarrier = new CyclicBarrier(5);
		CyclicBarrier endBarrier = new CyclicBarrier(5);

		// Tx1 Commit after chkpnt
		new SetValTx(startBarrier, endBarrier, blk, 204, new IntegerConstant(3538), 0, 2).start();
		// Tx2 Commit before chkpnt
		new SetValTx(startBarrier, endBarrier, blk, 220, new VarcharConstant("twel"), 2, 0).start();
		// Tx3 Rollback before chkpnt
		new SetValTx(startBarrier, endBarrier, blk, 240, new VarcharConstant("tfth"), 1, 0).start();
		// Tx4 never commit or rollback
		new SetValTx(startBarrier, endBarrier, blk, 304, new IntegerConstant(9323), 0, 0).start();

		// Wait for setValue running
		try {
			startBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		// Main thread create chkpnt
		Transaction chkpnt = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);

		VanillaDb.txMgr().createCheckpoint(chkpnt);
		chkpnt.commit();

		// Wait for Checkpoint running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		startBarrier = new CyclicBarrier(2);
		endBarrier = new CyclicBarrier(3);

		// Tx5 Commit after chkpnt
		new SetValTx(startBarrier, endBarrier, blk, 320, new VarcharConstant("sixt"), 2, 0).start();
		// Tx6 never commit or rollback
		new SetValTx(startBarrier, endBarrier, blk, 340, new VarcharConstant("eenth"), 0, 0).start();

		// Wait for tx5 commit running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		Transaction recoveryTx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr.recover(recoveryTx);
		Buffer buff = recoveryTx.bufferMgr().pin(blk);

		int ti1 = (Integer) buff.getVal(204, INTEGER).asJavaVal();
		String ts2 = (String) buff.getVal(220, VARCHAR).asJavaVal();
		String ts3 = (String) buff.getVal(240, VARCHAR).asJavaVal();
		int ti4 = (Integer) buff.getVal(304, INTEGER).asJavaVal();
		String ts5 = (String) buff.getVal(320, VARCHAR).asJavaVal();
		String ts6 = (String) buff.getVal(340, VARCHAR).asJavaVal();
		assertTrue("*****RecoveryTest: bad checkpoint recovery", ti1 == 3538 && ts2.equals("twel") && ts3.equals("urth")
				&& ti4 == 9265 && ts5.equals("sixt") && ts6.equals("ghth"));
		recoveryTx.bufferMgr().unpin(buff);

	}

	class SetValTx extends BarrierStartRunner {
		BlockId blk;
		int offset;
		Constant constant;
		Transaction tx;
		Buffer buff;
		RecoveryMgr rm;

		long txNum;
		int beforeTask = 0;
		int afterTask = 0;

		public SetValTx(CyclicBarrier startBarrier, CyclicBarrier endBarrier, BlockId blk, int offset,
				Constant constant, int beforeTask, int afterTask) {
			super(startBarrier, endBarrier);
			this.blk = blk;
			this.offset = offset;
			this.constant = constant;
			this.beforeTask = beforeTask;
			this.afterTask = afterTask;
		}

		@Override
		public void beforeTask() {
			tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
			txNum = tx.getTransactionNumber();
			buff = tx.bufferMgr().pin(blk);
			rm = tx.recoveryMgr();
			LogSeqNum lsn = rm.logSetVal(buff, offset, constant);
			buff.setVal(offset, this.constant, txNum, lsn);
			tx.bufferMgr().unpin(buff);

			doSomething(beforeTask);

		}

		@Override
		public void afterTask() {
			doSomething(afterTask);

		}

		@Override
		public void runTask() {

		}

		private void doSomething(int order) {
			switch (order) {
			case 0:
				// do not thing
				break;
			case 1:
				tx.rollback();
				break;
			case 2:
				tx.commit();
				break;
			}
		}

	}
	@Test
	public void testBTreeIndexRecovery() {

		Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		Map<String, IndexInfo> idxmap = md.getIndexInfo(dataTableName, tx);

		Index cidIndex = idxmap.get("cid").open(tx);
		RecordId[] records = new RecordId[10];
		BlockId blk = new BlockId(dataTableName + ".tbl", 0);
		Constant int5 = new IntegerConstant(5);

		for (int i = 0; i < 10; i++) {
			records[i] = new RecordId(blk, i);
			cidIndex.insert(int5, records[i], true);
		}

		RecordId rid2 = new RecordId(blk, 19);
		Constant int7 = new IntegerConstant(7);
		cidIndex.insert(int7, rid2, true);

		cidIndex.close();
		tx.commit();

		tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr.recover(tx);
		tx.commit();

		tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, true);
		idxmap = md.getIndexInfo(dataTableName, tx);
		cidIndex = idxmap.get("cid").open(tx);
		cidIndex.beforeFirst(ConstantRange.newInstance(int5));
		int k = 0;
		while (cidIndex.next())
			k++;

		assertTrue("*****RecoveryTest: bad index insertion recovery", k == 10);

		cidIndex.beforeFirst(ConstantRange.newInstance(int7));
		cidIndex.next();
		assertTrue("*****RecoveryTest: bad index insertion recovery", cidIndex.getDataRecordId().equals(rid2));

		cidIndex.close();
		tx.commit();

		// test roll back deletion on index
		tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		idxmap = md.getIndexInfo(dataTableName, tx);
		cidIndex = idxmap.get("cid").open(tx);
		cidIndex.delete(int7, rid2, true);

		RecordId rid3 = new RecordId(blk, 999);
		Constant int777 = new IntegerConstant(777);
		cidIndex.insert(int777, rid3, true);
		cidIndex.close();
		tx.rollback();

		tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, true);
		cidIndex.beforeFirst(ConstantRange.newInstance(int7));
		cidIndex.next();
		assertTrue("*****RecoveryTest: bad index deletion rollback", cidIndex.getDataRecordId().equals(rid2));

		cidIndex.beforeFirst(ConstantRange.newInstance(int777));
		cidIndex.next();
		assertTrue("*****RecoveryTest: bad index insertion rollback", !cidIndex.next());
		cidIndex.close();
		tx.commit();

	}

}
