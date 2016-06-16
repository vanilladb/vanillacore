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
package org.vanilladb.core.storage.index.btree;

import java.sql.Connection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

import junit.framework.Assert;

public class BTreeLeafTest {

	// the file starting with "_temp" will be deleted during initialization
	private static String FILE_PREFIX = "_test" + System.currentTimeMillis() + "_";
	private static final String INDEX_FILE_NAME = BTreeLeaf.getFileName(FILE_PREFIX + "BtreeLeaf");
	private static final BlockId DATA_BLOCK = new BlockId("_tempBtreeLeaf.tbl", 0);
	private static final Type KEY_TYPE = Type.INTEGER;
	private static final int MAX_NUM_OF_RECORDS;
	private static final int PRE_FORMATED_PAGE_COUNT = 4;
	
	static {
		int slotSize = BTreePage.slotSize(BTreeLeaf.schema(KEY_TYPE));
		int flagSize = BTreeLeaf.NUM_FLAGS * Type.INTEGER.maxSize();
		MAX_NUM_OF_RECORDS = (Buffer.BUFFER_SIZE - flagSize) / slotSize;
	}
	
	private Transaction tx;
	
	@BeforeClass
	public static void init() {
		ServerInit.init(BTreeLeafTest.class);
		RecoveryMgr.enableLogging(false);
		
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		
		// Format the pages that will be used later
		for (int i = 0; i < PRE_FORMATED_PAGE_COUNT; i++)
			tx.bufferMgr().pinNew(INDEX_FILE_NAME, new BTPageFormatter(BTreeLeaf.schema(KEY_TYPE), new long[]{-1, -1}));
		
		tx.commit();
	}
	
	@AfterClass
	public static void clean() {
		RecoveryMgr.enableLogging(true);
	}
	
	@Before
	public void createTx() {
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
	}
	
	@After
	public void finishTx() {
		tx.commit();
		tx = null;
	}
	
	@Test
	public void testInsert() {
		BlockId blk = new BlockId(INDEX_FILE_NAME, 0);
		BTreeLeaf leaf;
		ConstantRange insertKey;
		
		// Insert 20 records
		int numOfRecords = 20;
		for (int i = 0; i < numOfRecords; i++) {
			insertKey = ConstantRange.newInstance(new IntegerConstant(i));
			leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, insertKey, tx);
			leaf.insert(new RecordId(DATA_BLOCK, i));
			leaf.close();
		}
		
		// Check number of records
		ConstantRange searchRange = ConstantRange.newInstance(
				new IntegerConstant(0), true, new IntegerConstant(numOfRecords - 1), true);
		leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, searchRange, tx);
		Assert.assertEquals(numOfRecords, leaf.getNumRecords());
		
		// Check the order of records
		for (int i = 0; i < numOfRecords; i++) {
			if (!leaf.next())
				Assert.fail("BTreeLeafTest: Bad leaf.next()");
			
			Assert.assertEquals(new RecordId(DATA_BLOCK, i), leaf.getDataRecordId());
		}
	}
	
	@Test
	public void testDelete() {
		BlockId blk = new BlockId(INDEX_FILE_NAME, 1);
		BTreeLeaf leaf;
		ConstantRange insertKey;
		
		// Insert 20 records
		int numOfRecords = 20;
		for (int i = 0; i < numOfRecords; i++) {
			insertKey = ConstantRange.newInstance(new IntegerConstant(i));
			leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, insertKey, tx);
			leaf.insert(new RecordId(DATA_BLOCK, i));
			leaf.close();
		}
		
		// Check number of records
		ConstantRange searchRange = ConstantRange.newInstance(
				new IntegerConstant(0), true, new IntegerConstant(numOfRecords - 1), true);
		leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, searchRange, tx);
		Assert.assertEquals(numOfRecords, leaf.getNumRecords());
		
		// Check the order of records
		for (int i = 0; i < numOfRecords; i++) {
			insertKey = ConstantRange.newInstance(new IntegerConstant(i));
			leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, insertKey, tx);
			leaf.delete(new RecordId(DATA_BLOCK, i));
			leaf.close();
		}
		
		// Check number of records
		leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, searchRange, tx);
		Assert.assertEquals(0, leaf.getNumRecords());
	}
	
	@Test(timeout=1000)
	public void testOverflow() {
		BlockId blk = new BlockId(INDEX_FILE_NAME, 2);
		BTreeLeaf leaf;
		ConstantRange insertKey = ConstantRange.newInstance(new IntegerConstant(0));
		
		// Insert a lot of records with the same key
		int numOfRecords = MAX_NUM_OF_RECORDS * 3 / 2;
		for (int i = 0; i < numOfRecords; i++) {
			leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, insertKey, tx);
			leaf.insert(new RecordId(DATA_BLOCK, i));
			leaf.close();
		}
		
		// Check the number of data in both pages
		int count = 0;
		leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, insertKey, tx);
		while (leaf.next())
			count++;
		leaf.close();
		
		Assert.assertEquals(numOfRecords, count);
	}
	
	@Test
	public void testSplit() {
		BlockId blk = new BlockId(INDEX_FILE_NAME, 3);
		BlockId newBlk = null;
		BTreeLeaf leaf;
		DirEntry dirEntry;
		ConstantRange insertKey;
		
		// Insert a lot of records with the different keys
		int numOfRecords = MAX_NUM_OF_RECORDS * 3 / 2;
		for (int i = 0; i < numOfRecords; i++) {
			insertKey = ConstantRange.newInstance(new IntegerConstant(i));
			leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, insertKey, tx);
			dirEntry = leaf.insert(new RecordId(DATA_BLOCK, i));
			if (dirEntry != null)
				newBlk = new BlockId(INDEX_FILE_NAME, dirEntry.blockNumber());
			leaf.close();
		}
		
		if (newBlk == null)
			Assert.fail("BTreeLeafTest: Bad split");
		
		// Check the number of data in both pages
		ConstantRange searchRange = ConstantRange.newInstance(
				new IntegerConstant(0), true, new IntegerConstant(numOfRecords - 1), true);
		int count = 0;
		leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, searchRange, tx);
		while (leaf.next())
			count++;
		leaf.close();
		
		Assert.assertEquals(numOfRecords, count);
	}
}
