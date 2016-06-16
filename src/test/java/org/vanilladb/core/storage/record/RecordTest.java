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
package org.vanilladb.core.storage.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class RecordTest {
	private static Logger logger = Logger.getLogger(RecordTest.class.getName());

	private static Schema schema;
	private static TableInfo ti1, ti2;
	
	private static Transaction tx;
	
	private static String FILE_PREFIX = "_test" + System.currentTimeMillis() + "_";
	private static String tableName1 = FILE_PREFIX + "course1",
			tableName2 = FILE_PREFIX + "course2";
	
	@BeforeClass
	public static void init() {
		ServerInit.init(RecordTest.class);
		RecoveryMgr.enableLogging(false);

		schema = new Schema();
		schema.addField("cid", INTEGER);
		schema.addField("title", VARCHAR(20));
		schema.addField("deptid", BIGINT);
		ti1 = new TableInfo(tableName1, schema);
		ti2 = new TableInfo(tableName2, schema);
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN RECORD TEST");
	}
	
	@AfterClass
	public static void clean() {
		RecoveryMgr.enableLogging(true);
	}
	
	@After
	public void finishTx() {
		if (tx != null) {
			tx.commit();
			tx = null;
		}
	}

	@Test
	public void testReadOnly() {
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		
		String fileName = ti1.fileName();
		RecordFormatter fmtr = new RecordFormatter(ti1);
		Buffer buff = tx.bufferMgr().pinNew(fileName, fmtr);
		tx.bufferMgr().unpin(buff);
		RecordPage rp = new RecordPage(buff.block(), ti1, tx, true);
		try {
			rp.insertIntoNextEmptySlot();
			fail("RecordTest: bad readOnly");
		} catch (UnsupportedOperationException e) {

		}
		rp.close();
	}

	@Test
	public void testRecordPage() {
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		
		BufferMgr bufferMgr = tx.bufferMgr();
		RecordFormatter fmtr = new RecordFormatter(ti1);
		Buffer buff = bufferMgr.pinNew(ti1.fileName(), fmtr);
		BlockId blk = buff.block();
		bufferMgr.unpin(buff);
		
		RecordPage rp = new RecordPage(blk, ti1, tx, true);
		int startId = 0;
		RecordId dummyFreeSlot = new RecordId(new BlockId(ti1.fileName(), -1), -1);
		
		// Part 0: Delete existing records (if any)
		while (rp.next())
			rp.delete(dummyFreeSlot);
		rp = new RecordPage(blk, ti1, tx, true);

		// Part 1: Fill the page with some records
		int id = startId;
		int numinserted = 0;
		while (rp.insertIntoNextEmptySlot()) {
			rp.setVal("cid", new IntegerConstant(id));
			rp.setVal("deptid", new BigIntConstant((id % 3 + 1) * 10));
			rp.setVal("title", new VarcharConstant("course" + id));
			id++;
			numinserted++;
		}
		rp.close();

		// Part 2: Retrieve the records
		rp = new RecordPage(blk, ti1, tx, true);
		id = startId;
		while (rp.next()) {
			int cid = (Integer) rp.getVal("cid").asJavaVal();
			long deptid = (Long) rp.getVal("deptid").asJavaVal();
			String title = (String) rp.getVal("title").asJavaVal();
			assertTrue("RecordTest: bad page read",
					cid == id && title.equals("course" + id)
							&& deptid == (id % 3 + 1) * 10);
			id++;
		}
		rp.close();

		// Part 3: Modify the records
		rp = new RecordPage(blk, ti1, tx, true);
		id = startId;
		int numdeleted = 0;
		while (rp.next()) {
			if (rp.getVal("deptid").equals(new BigIntConstant(30))) {
				rp.delete(dummyFreeSlot);
				numdeleted++;
			}
		}
		rp.close();
		assertEquals("RecordTest: deleted wrong number of records from page",
				numinserted / 3, numdeleted);

		rp = new RecordPage(blk, ti1, tx, true);
		while (rp.next()) {
			assertNotSame("RecordTest: bad page delete", (Integer) 30,
					(Long) rp.getVal("deptid").asJavaVal());
		}
		rp.close();
	}

	@Test
	public void testRecordFile() {
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		
		// initial header page
		FileHeaderFormatter fhf = new FileHeaderFormatter();
		Buffer buff = tx.bufferMgr().pinNew(ti2.fileName(), fhf);
		tx.bufferMgr().unpin(buff);

		RecordFile rf = ti2.open(tx, true);
		int max = 300;

		// Part 0: Delete existing records (if any)
		rf.beforeFirst();
		while (rf.next())
			rf.delete();
		rf.close();

		// Part 1: Fill the file with lots of records
		rf = ti2.open(tx, true);
		for (int id = 0; id < max; id++) {
			rf.insert();
			rf.setVal("cid", new IntegerConstant(id));
			rf.setVal("title", new VarcharConstant("course" + id));
			rf.setVal("deptid", new BigIntConstant((id % 3 + 1) * 10));
		}
		rf.close();

		// Part 2: Retrieve the records
		int id = 0;
		rf = ti2.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			int cid = (Integer) rf.getVal("cid").asJavaVal();
			String title = (String) rf.getVal("title").asJavaVal();
			long deptid = (Long) rf.getVal("deptid").asJavaVal();
			assertTrue("RecordTest: bad file read",
					cid == id && title.equals("course" + id)
							&& deptid == (id % 3 + 1) * 10);
			id++;
		}
		rf.close();
		assertEquals("RecordTest: wrong number of records", max, id);

		// Part 3: Delete some of the records
		rf = ti2.open(tx, true);
		rf.beforeFirst();
		int numdeleted = 0;
		while (rf.next()) {
			if (rf.getVal("deptid").equals(new BigIntConstant(30))) {
				rf.delete();
				numdeleted++;
			}
		}
		assertEquals("RecordTest: wrong number of deletions", max / 3,
				numdeleted);

		// test that the deletions occurred
		rf.beforeFirst();
		while (rf.next()) {
			assertNotSame("RecordTest: not enough deletions", (Long) 30L,
					(Long) rf.getVal("deptid").asJavaVal());
		}
		rf.close();

		rf = ti2.open(tx, true);
		for (int i = 301; i < 405; i++) {
			rf.insert();
			rf.setVal("cid", new IntegerConstant(i));
			rf.setVal("title", new VarcharConstant("course" + id));
			rf.setVal("deptid", new BigIntConstant((id % 3 + 1) * 10));
		}
		rf.close();
	}
	
	
	/**
	 * This test case tests a special case. The case only happens when
	 * a tx tries to set a VARCHAR value at a inserted slot that was a 
	 * deleted slot.<br />
	 * 
	 * After a record is deleted, the slot of the record
	 * is filled with free chain information. If a slot was set to IN_USE 
	 * again, the free chain information might be still there. Note that
	 * before setting a value, we need to get the old value for logging.
	 * If we set a VARCHAR constant right at the free chain info, the 
	 * behavior of trying to get value with VARCHAR type would cause 
	 * NegativeArraySizeException at Page level.
	 * 
	 * @author yslin
	 */
	@Test
	public void testASpecialCase() {
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		
		Schema sch = new Schema();
		sch.addField("test_str", Type.VARCHAR(30));
		TableInfo ti = new TableInfo("test_table", sch);
		RecordFile.formatFileHeader(ti.fileName(), tx);
		
		// Insert a record
		RecordFile rf = ti.open(tx, true);
		rf.insert();
		rf.setVal("test_str", new VarcharConstant("testtesttesttesttest"));
		rf.close();
		
		// Delete a record
		rf = ti.open(tx, true);
		rf.beforeFirst();
		if (!rf.next())
			fail("RecordTest: bad insertion");
		rf.delete();
		rf.close();
		
		// Insert again
		rf = ti.open(tx, true);
		rf.insert();
		rf.setVal("test_str", new VarcharConstant("testtesttesttesttest"));
		rf.close();
	}
}
