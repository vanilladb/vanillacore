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
package org.vanilladb.core.storage.tx.concurrency;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.tx.Transaction;

public class ConcurrencyTest {
	private static Logger logger = Logger.getLogger(ConcurrencyTest.class
			.getName());

	private static String fileName = "_testconcurrency.0";
	private static int max = 100;
	private static BlockId[] blocks;
	private static Transaction tx1, tx2, tx3, tx4, tx5, tx6;
	private static ConcurrencyMgr scm1;
	private static ConcurrencyMgr scm2;
	private static ConcurrencyMgr rrcm1;
	private static ConcurrencyMgr rrcm2;
	private static ConcurrencyMgr rccm1;
	private static ConcurrencyMgr rccm2;

	@BeforeClass
	public static void init() {
		ServerInit.init(ConcurrencyTest.class);
		ServerInit.loadTestbed();

		blocks = new BlockId[max];
		for (int i = 0; i < max; i++)
			blocks[i] = new BlockId(fileName, i);
		tx1 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		tx2 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		tx3 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_REPEATABLE_READ, false);
		tx4 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_REPEATABLE_READ, false);
		tx5 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_READ_COMMITTED, false);
		tx6 = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_READ_COMMITTED, false);
		scm1 = tx1.concurrencyMgr();
		scm2 = tx2.concurrencyMgr();
		rrcm1 = tx3.concurrencyMgr();
		rrcm2 = tx4.concurrencyMgr();
		rccm1 = tx5.concurrencyMgr();
		rccm2 = tx6.concurrencyMgr();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN CONCURRENCY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH CONCURRENCY TEST");
	}

	@After
	public void teardown() {
		tx1.rollback();
		tx2.rollback();
		tx3.rollback();
		tx4.rollback();
		tx5.rollback();
		tx6.rollback();
	}

	@Test
	public void testSerializableConcurrencyMgr() {
		try {
			scm2.readFile(fileName);
			scm2.readBlock(blocks[1]);
			scm1.modifyBlock(blocks[1]);
			fail("*****ConcurrencyTest: bad serializable concurrency");
		} catch (LockAbortException e) {
			scm1.onTxRollback(tx1);
			scm2.onTxRollback(tx2);
		}

		try {
			scm2.readFile(fileName);
			scm1.readBlock(blocks[1]);
			scm2.readBlock(blocks[2]);
		} catch (LockAbortException e) {
			fail("*****ConcurrencyTest: bad serializable concurrency");
		}
		scm1.onTxRollback(tx1);
		scm2.onTxRollback(tx2);

		try {
			scm1.modifyBlock(blocks[1]);
			scm2.readBlock(blocks[2]);
		} catch (LockAbortException e) {
			fail("*****ConcurrencyTest: bad serializable concurrency");
		}
		scm1.onTxRollback(tx1);
		scm2.onTxRollback(tx2);
	}

	@Test
	public void testSerializablePhantom() {
		try {
			scm1.readBlock(blocks[0]);
			scm2.insertBlock(blocks[1]);
			fail("*****ConcurrencyTest: bad serializable concurrency");
		} catch (LockAbortException e) {
			scm1.onTxRollback(tx1);
			scm2.onTxRollback(tx2);
		}
	}

	@Test
	public void testRepeatableReadConcurrency() {
		try {
			rrcm1.readBlock(blocks[0]);
			rrcm2.modifyBlock(blocks[0]);
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		} catch (LockAbortException e) {
			rrcm1.onTxRollback(tx3);
			rrcm2.onTxRollback(tx4);
		}
	}

	@Test
	public void testRepeatableReadPhantom() {
		try {
			rrcm1.readFile(fileName);
			rrcm1.readBlock(blocks[1]);
			rrcm2.insertBlock(blocks[2]);
			rrcm2.modifyBlock(blocks[2]);
		} catch (LockAbortException e) {
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		}
		rrcm1.onTxRollback(tx3);
		rrcm2.onTxRollback(tx4);
	}

	@Test
	public void testReadCommittedEndStatement() {
		try {
			rccm1.readBlock(blocks[0]);
			rccm1.readBlock(blocks[1]);
			rccm1.onTxEndStatement(tx5);
			rccm2.modifyBlock(blocks[2]);
		} catch (LockAbortException e) {
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		}
		rccm1.onTxRollback(tx5);
		rccm2.onTxRollback(tx6);
	}

	@Test
	public void testSS() {
		try {
			Plan p1 = new TablePlan("student", tx1);
			Scan s1 = p1.open();
			s1.beforeFirst();
			Plan p2 = new TablePlan("student", tx2);
			UpdateScan s2 = (UpdateScan) p2.open();
			s2.beforeFirst();
			while (s1.next())
				s2.next();
			s2.insert();
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		} catch (LockAbortException e) {

		}

		try {
			Plan p1 = new TablePlan("student", tx1);
			UpdateScan s1 = (UpdateScan) p1.open();
			s1.insert();
			Plan p2 = new TablePlan("student", tx2);
			UpdateScan s2 = (UpdateScan) p2.open();
			s2.insert();
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		} catch (LockAbortException e) {

		}
		tx1.rollback();
		tx2.rollback();
	}

	@Test
	public void testSRR() {
		/*
		 * if there is an unexpected lock abort exception, check that does tx3
		 * refresh the statistical information and lock all the block during
		 * constructing the table plan.
		 */
		try {
			Plan p1 = new TablePlan("student", tx3); // RR
			Scan s1 = p1.open();
			s1.beforeFirst();
			Plan p2 = new TablePlan("student", tx2); // S
			UpdateScan s2 = (UpdateScan) p2.open();
			s2.beforeFirst();
			s1.next(); // tx3 islock file then release
			s2.next(); // tx2 islock file
			s2.insert();
		} catch (LockAbortException e) {
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		}
		tx2.rollback();
		tx3.rollback();

		try {
			Plan p1 = new TablePlan("student", tx3); // RR
			UpdateScan s1 = (UpdateScan) p1.open();
			s1.beforeFirst();
			Plan p2 = new TablePlan("student", tx2); // S
			Scan s2 = p2.open();
			s2.beforeFirst();
			s1.next();
			s2.next();
			s1.insert();
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		} catch (LockAbortException e) {

		}
		tx2.rollback();
		tx3.rollback();
	}

	@Test
	public void testSRC() {
		try {
			Plan p1 = new TablePlan("student", tx5); // RC
			Scan s1 = p1.open();
			s1.beforeFirst();
			Plan p2 = new TablePlan("student", tx2); // S
			UpdateScan s2 = (UpdateScan) p2.open();
			s2.beforeFirst();
			while (s1.next())
				s1.getVal("sid"); // S lock all blocks
			rccm1.onTxRollback(tx5);
			s2.next();
			s2.insert();
		} catch (LockAbortException e) {
			fail("*****ConcurrencyTest: bad read committed concurrency");
		}
		tx5.rollback();
		tx2.rollback();
	}
}
