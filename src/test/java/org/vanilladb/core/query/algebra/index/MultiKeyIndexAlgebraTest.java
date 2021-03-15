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
package org.vanilladb.core.query.algebra.index;

import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.predicate.ConstantExpression;
import org.vanilladb.core.sql.predicate.Expression;
import org.vanilladb.core.sql.predicate.FieldNameExpression;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;

import org.junit.Assert;

public class MultiKeyIndexAlgebraTest {
	private static Logger logger = Logger.getLogger(MultiKeyIndexAlgebraTest.class.getName());
	
	private static final String TABLE_NAME = "testing_table";
	private static final String INDEX_NAME = "testing_index";
	private static final String JOIN_TABLE_NAME = "testing_join_table";
	private static final int KEY_MAX = 20;
	
	@BeforeClass
	public static void init() {
		ServerInit.init(MultiKeyIndexAlgebraTest.class);
		
		generateTestingData();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN MULTI-KEY INDEXES QUERY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH MULTI-KEY INDEXES QUERY TEST");
	}
	
	private static void generateTestingData() {
		if (logger.isLoggable(Level.INFO))
			logger.info("loading data");
		
		CatalogMgr cataMgr = VanillaDb.catalogMgr();
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		
		// Create a table
		Schema sch = new Schema();
		sch.addField("key_1", Type.INTEGER);
		sch.addField("key_2", Type.INTEGER);
		sch.addField("key_3", Type.INTEGER);
		sch.addField("data", Type.VARCHAR(100));
		cataMgr.createTable(TABLE_NAME, sch, tx);
		
		// Create a multi-key index
		List<String> indexedFlds = new LinkedList<String>();
		indexedFlds.add("key_1");
		indexedFlds.add("key_2");
		indexedFlds.add("key_3");
		cataMgr.createIndex(INDEX_NAME, TABLE_NAME, indexedFlds,
				IndexType.BTREE, tx);
		
		// Load data
		TableInfo ti = cataMgr.getTableInfo(TABLE_NAME, tx);
		IndexInfo ii = cataMgr.getIndexInfoByName(INDEX_NAME, tx);
		RecordFile rf = ti.open(tx, true);
		Index idx = ii.open(tx);
		
		for (int key1 = 1; key1 <= KEY_MAX; key1++) {
			for (int key2 = 1; key2 <= KEY_MAX; key2++) {
				for (int key3 = 1; key3 <= KEY_MAX; key3++) {
					
					// Insert a record
					rf.insert();
					rf.setVal("key_1", new IntegerConstant(key1));
					rf.setVal("key_2", new IntegerConstant(key2));
					rf.setVal("key_3", new IntegerConstant(key3));
					rf.setVal("data", toTestingData(key1, key2, key3));
					
					// Insert an index record
					SearchKey key = new SearchKey(new IntegerConstant(key1),
							new IntegerConstant(key2), new IntegerConstant(key3));
					idx.insert(key, rf.currentRecordId(), true);
				}
			}
		}
		
		// Finish
		rf.close();
		idx.close();
		
		// Create a join table
		sch = new Schema();
		sch.addField("join_key_1", Type.INTEGER);
		sch.addField("join_key_2", Type.INTEGER);
		sch.addField("join_key_3", Type.INTEGER);
		sch.addField("join_data", Type.VARCHAR(100));
		cataMgr.createTable(JOIN_TABLE_NAME, sch, tx);
		
		// Load data
		ti = cataMgr.getTableInfo(JOIN_TABLE_NAME, tx);
		rf = ti.open(tx, true);
		
		for (int key1 = 1; key1 <= KEY_MAX; key1++) {
			for (int key2 = 1; key2 <= KEY_MAX; key2++) {
				for (int key3 = 1; key3 <= KEY_MAX; key3++) {
					// Insert a record
					rf.insert();
					rf.setVal("join_key_1", new IntegerConstant(key1));
					rf.setVal("join_key_2", new IntegerConstant(key2));
					rf.setVal("join_key_3", new IntegerConstant(key3));
					rf.setVal("join_data", toTestingJoinData(key1, key2, key3));
				}
			}
		}
		
		// Finish
		rf.close();
		
		tx.commit();
	}
	
	private static VarcharConstant toTestingData(int key1, int key2, int key3) {
		return new VarcharConstant(String.format("test_%d_%d_%d", key1, key2, key3));
	}
	
	private static VarcharConstant toTestingJoinData(int key1, int key2, int key3) {
		return new VarcharConstant(String.format("join_test_%d_%d_%d", key1, key2, key3));
	}
	
	private Transaction tx;
	
	@Before
	public void createTx() {
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
	}

	@After
	public void finishTx() {
		tx.commit();
	}
	
	/**
	 * Select the record with key {1, 2, 3}
	 */
	@Test
	public void testPreciseSelection() {
		TablePlan tp = new TablePlan(TABLE_NAME, tx);
		IndexInfo ii = VanillaDb.catalogMgr().getIndexInfoByName(INDEX_NAME, tx);
		Map<String, ConstantRange> searchRanges = new HashMap<String, ConstantRange>();
		
		Constant key1 = new IntegerConstant(1);
		Constant key2 = new IntegerConstant(2);
		Constant key3 = new IntegerConstant(3);
		Constant data = toTestingData(1, 2, 3);
		
		searchRanges.put("key_1", ConstantRange.newInstance(key1));
		searchRanges.put("key_2", ConstantRange.newInstance(key2));
		searchRanges.put("key_3", ConstantRange.newInstance(key3));
		
		IndexSelectPlan plan = new IndexSelectPlan(tp, ii, searchRanges, tx);
		Scan scan = plan.open();
		
		scan.beforeFirst();
		
		if (!scan.next())
			Assert.fail("*****MultiKeyIndexTest: could not find the record");
		
		Assert.assertEquals(key1, scan.getVal("key_1"));
		Assert.assertEquals(key2, scan.getVal("key_2"));
		Assert.assertEquals(key3, scan.getVal("key_3"));
		Assert.assertEquals(data, scan.getVal("data"));
		
		if (scan.next())
			Assert.fail("*****MultiKeyIndexTest: wrong count");
		
		scan.close();
	}
	
	/**
	 * Select the records with key {1, *, *}
	 */
	@Test
	public void testRangeSelection() {
		TablePlan tp = new TablePlan(TABLE_NAME, tx);
		IndexInfo ii = VanillaDb.catalogMgr().getIndexInfoByName(INDEX_NAME, tx);
		Map<String, ConstantRange> searchRanges = new HashMap<String, ConstantRange>();
		
		searchRanges.put("key_1", ConstantRange.newInstance(new IntegerConstant(1)));
		
		IndexSelectPlan plan = new IndexSelectPlan(tp, ii, searchRanges, tx);
		Scan scan = plan.open();
		
		scan.beforeFirst();
		
		int count = 0;
		while (scan.next())
			count++;
		scan.close();
		
		Assert.assertEquals("*****MultiKeyIndexTest: wrong count", KEY_MAX * KEY_MAX, count);
	}
	
	/**
	 * Select the records with key {1, 2, 3} in the both table
	 */
	@Test
	public void testIndexJoin() {
		// Create a SelectPlan for the first table
		Expression exp1 = new FieldNameExpression("join_key_1");
		Expression exp2 = new ConstantExpression(new IntegerConstant(1));
		Term t = new Term(exp1, Term.OP_EQ, exp2);
		Predicate pred = new Predicate(t);
		
		exp1 = new FieldNameExpression("join_key_2");
		exp2 = new ConstantExpression(new IntegerConstant(2));
		t = new Term(exp1, Term.OP_EQ, exp2);
		pred.conjunctWith(t);
		
		exp1 = new FieldNameExpression("join_key_3");
		exp2 = new ConstantExpression(new IntegerConstant(3));
		t = new Term(exp1, Term.OP_EQ, exp2);
		pred.conjunctWith(t);
		
		TablePlan tp = new TablePlan(JOIN_TABLE_NAME, tx);
		Plan p = new SelectPlan(tp, pred);
		
		// Create a mapping for joined field names
		Map<String, String> joinFields = new HashMap<String, String>();
		joinFields.put("join_key_1", "key_1");
		joinFields.put("join_key_2", "key_2");
		joinFields.put("join_key_3", "key_3");
		
		// Create an IndexJoinPlan
		tp = new TablePlan(TABLE_NAME, tx);
		IndexInfo ii = VanillaDb.catalogMgr().getIndexInfoByName(INDEX_NAME, tx);
		p = new IndexJoinPlan(p, tp, ii, joinFields, tx);
		
		// Open the scan
		Scan scan = p.open();
		scan.beforeFirst();
		
		if (!scan.next())
			Assert.fail("*****MultiKeyIndexTest: could not find the record");
		
		Assert.assertEquals(new IntegerConstant(1), scan.getVal("key_1"));
		Assert.assertEquals(new IntegerConstant(2), scan.getVal("key_2"));
		Assert.assertEquals(new IntegerConstant(3), scan.getVal("key_3"));
		Assert.assertEquals(toTestingData(1, 2, 3), scan.getVal("data"));
		Assert.assertEquals(toTestingJoinData(1, 2, 3), scan.getVal("join_data"));
		
		if (scan.next())
			Assert.fail("*****MultiKeyIndexTest: wrong count");
		
		scan.close();
	}
}
