/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

public class TxTest {
	private static Logger logger = Logger.getLogger(TxTest.class.getName());

	// For testing recovery, filename cannot start with "_temp"
	private static String FILE_PREFIX = "_test" + System.currentTimeMillis() + "_";
	private static String result;
	
	public static final String FILE_NAME = FILE_PREFIX + "tx";
	
	// Some testing values
	public static final Constant INT_0 = new IntegerConstant(0);
	private static final Constant INT_555 = new IntegerConstant(555);
	private static final Constant INT_9999 = new IntegerConstant(9999);

	@BeforeClass
	public static void init() {
		ServerInit.init(TxTest.class);

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN TX TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH TX TEST");
	}

	@Before
	public void setup() {
		result = "";
	}

	@Test
	public void testCommit() {
		// Tx1 write 9999 at 0
		Transaction tx1 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		BlockId blk = new BlockId(FILE_NAME, 0);
		Buffer buff = tx1.bufferMgr().pin(blk);
		tx1.concurrencyMgr().modifyBlock(blk);
		LogSeqNum lsn = tx1.recoveryMgr().logSetVal(buff, 0, INT_9999);
		buff.setVal(0, INT_9999, tx1.getTransactionNumber(), lsn);
		tx1.commit();
		
		// Tx2 read at 0 (it should be 9999)
		Transaction tx2 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		buff = tx2.bufferMgr().pin(blk);
		try {
			tx2.concurrencyMgr().readBlock(blk);
		} catch (LockAbortException e) {
			fail("TxTest: bad commit");
		}
		assertEquals("TxTest: bad commit", INT_9999, buff.getVal(0, INTEGER));
		tx2.commit();
	}

	@Test
	public void testRollback() {
		// Tx1 write 555 at 0
		Transaction tx1 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		BlockId blk = new BlockId(FILE_NAME, 1);
		Buffer buff = tx1.bufferMgr().pin(blk);
		buff.setVal(0, INT_555, tx1.getTransactionNumber(), null);
		tx1.bufferMgr().flushAll();
		tx1.commit();
		
		// Tx2 write 9999 at 0, but it rolls back
		Transaction tx2 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		buff = tx2.bufferMgr().pin(blk);
		tx2.concurrencyMgr().modifyBlock(blk);
		LogSeqNum lsn = tx2.recoveryMgr().logSetVal(buff, 0, INT_9999);
		buff.setVal(0, INT_9999, tx2.getTransactionNumber(), lsn);
		tx2.rollback();
		
		// Tx3 read at 0 (it should be 555)
		Transaction tx3 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		buff = tx3.bufferMgr().pin(blk);
		try {
			tx3.concurrencyMgr().readBlock(blk);
		} catch (LockAbortException e) {
			fail("TxTest: bad rollback");
		}
		assertEquals("TxTest: bad rollback", INT_555, buff.getVal(0, INTEGER));
		tx3.commit();
	}

	@Test
	public void testEndStatement() {
		// RC-Tx1 releases locks when ending a statement
		BlockId blk = new BlockId(FILE_NAME, 2);
		Transaction tx1 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_READ_COMMITTED, false);
		Buffer buff = tx1.bufferMgr().pin(blk);
		tx1.concurrencyMgr().readBlock(blk);
		buff.getVal(0, INTEGER);
		tx1.endStatement();
		
		// SS-Tx2 should be able to lock the object
		Transaction tx2 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		buff = tx2.bufferMgr().pin(blk);
		try {
			tx2.concurrencyMgr().modifyBlock(blk);
		} catch (LockAbortException e) {
			fail("TxTest: bad end statement");
		}
		
		// Commit
		tx2.commit();
		tx1.commit();
	}

	@Test
	public void testConcurrency() {
		TxClientA thA = new TxClientA(0, 600);
		thA.start();
		TxClientD thD = new TxClientD(200, 800);
		thD.start();
		TxClientC thC = new TxClientC(400, 400);
		thC.start();
		try {
			thA.join();
			thD.join();
			thC.join();
		} catch (InterruptedException e) {
		}
		String expected = "Tx A: read 1 start\n" + "Tx A: read 1 end\n"
				+ "Tx D: write 1 start\n" + "Tx C: read 1 start\n"
				+ "Tx C: read 1 end\n" + "Tx A: read 2 start\n"
				+ "Tx A: read 2 end\n" + "Tx C: write 2 start\n"
				+ "Tx C: write 2 end\n" + "Tx D: write 1 end\n"
				+ "Tx D: read 2 start\n" + "Tx D: read 2 end\n";
		assertEquals("TxTest: bad tx history", expected, result);
	}

	@Test
	public void testDeadlock() {
		TxClientB thB = new TxClientB(0, 400);
		thB.start();
		TxClientC thC = new TxClientC(200, 400);
		thC.start();
		try {
			thB.join();
			thC.join();
		} catch (InterruptedException e) {
		}
		String expected = "Tx B: write 2 start\n" + "Tx B: write 2 end\n"
				+ "Tx C: read 1 start\n" + "Tx C: read 1 end\n"
				+ "Tx B: write 1 start\n" + "Tx C: write 2 start\n"
				+ "Tx B: write 1 end\n";
		assertEquals("TxTest: bad tx history", expected, result);
		assertTrue("TxTest: bad tx history", !thB.isDeadlockAborted());
		assertTrue("TxTest: bad tx history", thC.isDeadlockAborted());
	}

	synchronized static void appendToResult(String s) {
		result += s + "\n";
	}
}

abstract class TxClient extends Thread {
	
	protected int[] pauses;
	protected boolean deadlockAborted;
	protected Transaction tx;

	TxClient(int... pauses) {
		this.pauses = pauses;
		this.tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
	}
	
	@Override
	public void run() {
		try {
			if (pauses[0] > 0)
				Thread.sleep(pauses[0]);

			runTask1();

			if (pauses[1] > 0)
				Thread.sleep(pauses[1]);

			runTask2();
			
		} catch (InterruptedException e) {
		} catch (LockAbortException e) {
			deadlockAborted = true;
		} finally {
			tx.rollback();
		}
	}
	
	abstract void runTask1();
	
	abstract void runTask2();

	boolean isDeadlockAborted() {
		return deadlockAborted;
	}
}

class TxClientA extends TxClient {
	TxClientA(int... pauses) {
		super(pauses);
	}
	
	void runTask1() {
		BlockId blk1 = new BlockId(TxTest.FILE_NAME, 0);
		TxTest.appendToResult("Tx A: read 1 start");
		tx.concurrencyMgr().readBlock(blk1);
		TxTest.appendToResult("Tx A: read 1 end");
	}
	
	void runTask2() {
		BlockId blk2 = new BlockId(TxTest.FILE_NAME, 1);
		TxTest.appendToResult("Tx A: read 2 start");
		tx.concurrencyMgr().readBlock(blk2);
		TxTest.appendToResult("Tx A: read 2 end");
	}
}

class TxClientB extends TxClient {
	TxClientB(int... pauses) {
		super(pauses);
	}
	
	void runTask1() {
		BlockId blk2 = new BlockId(TxTest.FILE_NAME, 1);
		TxTest.appendToResult("Tx B: write 2 start");
		tx.concurrencyMgr().modifyBlock(blk2);
		TxTest.appendToResult("Tx B: write 2 end");
	}
	
	void runTask2() {
		BlockId blk1 = new BlockId(TxTest.FILE_NAME, 0);
		TxTest.appendToResult("Tx B: write 1 start");
		tx.concurrencyMgr().modifyBlock(blk1);
		TxTest.appendToResult("Tx B: write 1 end");
	}
}

class TxClientC extends TxClient {
	TxClientC(int... pauses) {
		super(pauses);
	}
	
	void runTask1() {
		BlockId blk1 = new BlockId(TxTest.FILE_NAME, 0);
		TxTest.appendToResult("Tx C: read 1 start");
		tx.concurrencyMgr().readBlock(blk1);
		TxTest.appendToResult("Tx C: read 1 end");
	}
	
	void runTask2() {
		BlockId blk2 = new BlockId(TxTest.FILE_NAME, 1);
		TxTest.appendToResult("Tx C: write 2 start");
		tx.concurrencyMgr().modifyBlock(blk2);
		TxTest.appendToResult("Tx C: write 2 end");
	}
}

class TxClientD extends TxClient {
	TxClientD(int... pauses) {
		super(pauses);
	}
	
	void runTask1() {
		BlockId blk1 = new BlockId(TxTest.FILE_NAME, 0);
		TxTest.appendToResult("Tx D: write 1 start");
		tx.concurrencyMgr().modifyBlock(blk1);
		TxTest.appendToResult("Tx D: write 1 end");
	}
	
	void runTask2() {
		BlockId blk2 = new BlockId(TxTest.FILE_NAME, 1);
		TxTest.appendToResult("Tx D: read 2 start");
		tx.concurrencyMgr().readBlock(blk2);
		TxTest.appendToResult("Tx D: read 2 end");
	}
}
