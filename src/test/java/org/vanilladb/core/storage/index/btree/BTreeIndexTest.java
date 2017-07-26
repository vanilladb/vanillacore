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

import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

import junit.framework.Assert;

public class BTreeIndexTest {
	private static Logger logger = Logger.getLogger(BTreeIndexTest.class
			.getName());
	
	private static String FILE_PREFIX = "_test" + System.currentTimeMillis() + "_";
	private static final String DATA_TABLE_NAME = FILE_PREFIX + "BtreeData";
	
	private static CatalogMgr catMgr;
	
	private Transaction tx;

	@BeforeClass
	public static void init() {
		ServerInit.init(BTreeIndexTest.class);
		RecoveryMgr.enableLogging(false);
		catMgr = VanillaDb.catalogMgr();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN BTREE INDEX TEST");

		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Schema schema = new Schema();
		schema.addField("cid", INTEGER);
		schema.addField("title", VARCHAR(20));
		schema.addField("deptid", INTEGER);
		schema.addField("majorid", BIGINT);
		catMgr.createTable(DATA_TABLE_NAME, schema, tx);
		
		List<String> idxFlds1 = new LinkedList<String>();
		idxFlds1.add("cid");
		List<String> idxFlds2 = new LinkedList<String>();
		idxFlds2.add("title");
		List<String> idxFlds3 = new LinkedList<String>();
		idxFlds3.add("deptid");
		List<String> idxFlds4 = new LinkedList<String>();
		idxFlds4.add("majorid");
		
		catMgr.createIndex("_tempI1", DATA_TABLE_NAME, idxFlds1, IndexType.BTREE, tx);
		catMgr.createIndex("_tempI2", DATA_TABLE_NAME, idxFlds2, IndexType.BTREE, tx);
		catMgr.createIndex("_tempI3", DATA_TABLE_NAME, idxFlds3, IndexType.BTREE, tx);
		catMgr.createIndex("_tempI4", DATA_TABLE_NAME, idxFlds4, IndexType.BTREE, tx);
		tx.commit();
	}
	
	@AfterClass
	public static void finish() {
		RecoveryMgr.enableLogging(true);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH BTREE INDEX TEST");
	}

	@Before
	public void createTx() {
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
	}

	@After
	public void finishTx() {
		tx.commit();
	}

	@Test
	public void testBasicOperation() {
		List<IndexInfo> idxList = catMgr.getIndexInfo(DATA_TABLE_NAME, "cid", tx);
		Index cidIndex = idxList.get(0).open(tx);
		
		// Insert 10 records with the same key
		RecordId[] records = new RecordId[10];
		BlockId blk = new BlockId(DATA_TABLE_NAME + ".tbl", 0);
		SearchKey int5 = new SearchKey(new IntegerConstant(5));
		for (int i = 0; i < 10; i++) {
			records[i] = new RecordId(blk, i);
			cidIndex.insert(int5, records[i], false);
		}
		
		// Insert a record with another key
		RecordId rid2 = new RecordId(blk, 9);
		SearchKey int7 = new SearchKey(new IntegerConstant(7));
		cidIndex.insert(int7, rid2, false);
		
		// It should find 10 records for int 5
		cidIndex.beforeFirst(new SearchRange(int5));
		int k = 0;
		while (cidIndex.next())
			k++;
		Assert.assertEquals("*****BTreeIndexTest: bad insert", 10, k);
		
		// It should find only one record for int 7
		cidIndex.beforeFirst(new SearchRange(int7));
		cidIndex.next();
		assertTrue("*****BTreeIndexTest: bad read index", cidIndex
				.getDataRecordId().equals(rid2));
		
		// Delete the 10 records with key int 5
		for (int i = 0; i < 10; i++)
			cidIndex.delete(int5, records[i], false);
		cidIndex.beforeFirst(new SearchRange(int5));
		assertTrue("*****BTreeIndexTest: bad delete", cidIndex.next() == false);
		
		// Delete the record with key int 7
		cidIndex.delete(int7, rid2, false);
		cidIndex.close();
	}

	@Test
	public void testVarcharKey() {
		List<IndexInfo> idxList = catMgr.getIndexInfo(DATA_TABLE_NAME, "title", tx);
		Index cidIndex = idxList.get(0).open(tx);

		BlockId blk = new BlockId(DATA_TABLE_NAME + ".tbl", 0);

		int repeat = 1000;
		String str1 = "BAEBAEBAEBASAEBASE";
		String str3 = "AAEBAEBAEBASAEBASZ";
		String str4 = "BAEBAEBAEBASAEBASZ1";
		String str2 = "KARBAEBAEBASAEBASE";
		SearchKey key1 = new SearchKey(new VarcharConstant(str1, VARCHAR(20)));
		SearchKey key2 = new SearchKey(new VarcharConstant(str2, VARCHAR(20)));
		SearchKey key3 = new SearchKey(new VarcharConstant(str3, VARCHAR(20)));
		SearchKey key4 = new SearchKey(new VarcharConstant(str4, VARCHAR(20)));

		for (int i = 0; i < repeat; i++) {
			cidIndex.insert(key1, new RecordId(blk, i), false);
			cidIndex.insert(key2, new RecordId(blk, repeat + i), false);
			cidIndex.insert(key3, new RecordId(blk, repeat * 2 + i), false);
			cidIndex.insert(key4, new RecordId(blk, repeat * 3 + i), false);
		}

		cidIndex.beforeFirst(new SearchRange(key1));
		int j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: varchar selection", j == repeat);

		for (int i = 0; i < repeat; i++) {
			cidIndex.delete(key1, new RecordId(blk, i), false);
			cidIndex.delete(key2, new RecordId(blk, repeat + i), false);
			cidIndex.delete(key3, new RecordId(blk, repeat * 2 + i), false);
			cidIndex.delete(key4, new RecordId(blk, repeat * 3 + i), false);
		}

		cidIndex.close();
	}

	@Test
	public void testDir() {
		List<IndexInfo> idxList = catMgr.getIndexInfo(DATA_TABLE_NAME, "majorid", tx);
		Index cidIndex = idxList.get(0).open(tx);
		BlockId blk1 = new BlockId(DATA_TABLE_NAME + ".tbl", 0);
		int maxValue = 250; // 40000000
		/*
		 * for 4K block, int value: repeat same val 250 times may create
		 * overflow blk if repeat =200, the btree will create new node at k< 250
		 * and k<250*250. bigint: 200 will overflow. make new node at <40000
		 */
		int repeat = 170;
		for (int k = 0; k < maxValue; k++) {
			SearchKey key = new SearchKey(new BigIntConstant(k));
			for (int i = 0; i < repeat; i++) {
				cidIndex.insert(key, new RecordId(blk1, k * repeat + i), false);
			}
		}

		SearchKey int100 = new SearchKey(new IntegerConstant(100));
		SearchRange range100 = new SearchRange(int100);
		cidIndex.beforeFirst(range100);
		int j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: bad equal with", j == repeat);

		for (int i = 0; i < repeat; i++) {
			cidIndex.delete(int100, new RecordId(blk1, 100 * repeat + i), false);
		}
		cidIndex.beforeFirst(range100);
		assertTrue("*****BTreeIndexTest: bad delete", cidIndex.next() == false);
	}

	@Test
	public void testBTreeIndex() {
		List<IndexInfo> idxList = catMgr.getIndexInfo(DATA_TABLE_NAME, "deptid", tx);
		Index cidIndex = idxList.get(0).open(tx);
		BlockId blk = new BlockId(DATA_TABLE_NAME + ".tbl", 0);
		BlockId blk1 = new BlockId(DATA_TABLE_NAME + ".tbl", 23);
		int maxValue = 300;
		int repeat = 200;
		for (int k = 0; k < maxValue; k++) {
			for (int i = 0; i < repeat; i++) {
				SearchKey key = new SearchKey(new BigIntConstant(k));
				cidIndex.insert(key, new RecordId(blk, k * repeat + i), false);
			}
		}

		int count = 0;
		SearchKey int7 = new SearchKey(new IntegerConstant(7));
		while (count < 500) {
			cidIndex.insert(int7, new RecordId(blk1, 2500 + count), false);
			count++;
		}

		// test larger than 50
		ConstantRange range = ConstantRange.newInstance(new IntegerConstant(50),
				false, null, false);
		cidIndex.beforeFirst(new SearchRange(range));
		int j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: bad > selection", j == (maxValue - 51)
				* repeat);

		Constant int5con = new IntegerConstant(5);
		// test less than
		range = ConstantRange.newInstance(null, false, int5con, false);
		cidIndex.beforeFirst(new SearchRange(range));
		j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: bad < selection", j == (5 * repeat));

		// test equality
		cidIndex.beforeFirst(new SearchRange(new SearchKey(int5con)));
		j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: bad equal with", j == repeat);

		// test delete
		for (int k = 0; k < maxValue; k++) {
			for (int i = 0; i < repeat; i++) {
				SearchKey key = new SearchKey(new BigIntConstant(k));
				cidIndex.delete(key, new RecordId(blk, k * repeat + i), false);
			}
		}
		cidIndex.beforeFirst(new SearchRange(new SearchKey(int5con)));
		assertTrue("*****BTreeIndexTest: bad delete", cidIndex.next() == false);

		count = 0;
		while (count < 500) {
			cidIndex.delete(int7, new RecordId(blk1, 2500 + count), false);
			count++;
		}
		cidIndex.beforeFirst(new SearchRange(int7));
		assertTrue("*****BTreeIndexTest: bad delete", cidIndex.next() == false);

		cidIndex.close();
	}
}
