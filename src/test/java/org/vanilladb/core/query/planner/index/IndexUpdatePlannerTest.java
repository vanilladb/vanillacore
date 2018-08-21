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
package org.vanilladb.core.query.planner.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.query.planner.opt.HeuristicQueryPlanner;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.tx.Transaction;

public class IndexUpdatePlannerTest {
	private static Logger logger = Logger.getLogger(IndexUpdatePlannerTest.class.getName());
	
	private static final String TABLE_NAME = "indextest";
	
	@BeforeClass
	public static void init() {
		ServerInit.init(IndexUpdatePlannerTest.class);
		
		// create and populate the indexed temp table
		CatalogMgr md = VanillaDb.catalogMgr();
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Schema sch = new Schema();
		sch.addField("tid", INTEGER);
		sch.addField("tname", VARCHAR(10));
		sch.addField("tdate", BIGINT);
		md.createTable(TABLE_NAME, sch, tx);
		
		List<String> indexedFlds = new LinkedList<String>();
		indexedFlds.add("tid");
		md.createIndex("_tempIUP1", TABLE_NAME, indexedFlds, IndexType.BTREE, tx);
		
		indexedFlds = new LinkedList<String>();
		indexedFlds.add("tdate");
		md.createIndex("_tempIUP2", TABLE_NAME, indexedFlds, IndexType.BTREE, tx);

		tx.commit();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN INDEX UPDATE PLANNER TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH INDEX UPDATE PLANNER TEST");
	}
	
	private Transaction tx;
	
	@Before
	public void createTx() {
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
	}

	@After
	public void finishTx() {
		tx.rollback();
	}
	
	@Test
	public void testInsert() {
		String cmd = "INSERT INTO indextest(tid, tname, tdate) VALUES "
				+ "(1, 'basketry', 9890033330000)";
		Parser psr = new Parser(cmd);
		InsertData id = (InsertData) psr.updateCommand();
		int n = new IndexUpdatePlanner().executeInsert(id, tx);
		if (n != 1)
			assertEquals(
					"*****IndexUpdatePlannerTest: bad insertion return value",
					1, n);

		String qry = "select tid, tname, tdate from indextest where tid = 1";
		psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		Scan s = p.open();
		s.beforeFirst();
		int insertcount = 0;

		while (s.next()) {
			assertEquals("*****IndexUpdatePlannerTest: bad insert retrieval",
					(Integer) 1, (Integer) s.getVal("tid").asJavaVal());
			assertEquals("*****IndexUpdatePlannerTest: bad insert retrieval",
					"basketry", (String) s.getVal("tname").asJavaVal());
			assertEquals("*****IndexUpdatePlannerTest: bad insert retrieval",
					(Long) 9890033330000L, (Long) s.getVal("tdate").asJavaVal());
			insertcount++;
		}
		s.close();
		assertEquals("*****IndexUpdatePlannerTest: bad insertion count", 1,
				insertcount);
	}

	@Test
	public void testDelete() {
		// Insert the data that will be deleted
		for (int tid = 2; tid < 5; tid++) {
			String cmd = "insert into indextest(tid,tname,tdate) values(" + tid
					+ ", 'test" + tid + "', 1000000" + tid + ")";
			Parser psr = new Parser(cmd);
			InsertData id = (InsertData) psr.updateCommand();
			new IndexUpdatePlanner().executeInsert(id, tx);
		}

		// Execute deletion
		String cmd = "delete from indextest where tid > 1";
		Parser psr = new Parser(cmd);
		DeleteData dd = (DeleteData) psr.updateCommand();
		IndexUpdatePlanner iup = new IndexUpdatePlanner();
		iup.executeDelete(dd, tx);

		// Check if the data has been deleted
		String qry = "select tid from indextest where tid > 1";
		psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		Scan s = p.open();
		s.beforeFirst();
		while (s.next())
			fail("*****IndexUpdatePlannerTest: bad delete");
		s.close();
	}

	@Test
	public void testModify() {
		// Insert the data that will be deleted
		for (int tid = 2; tid < 5; tid++) {
			String cmd = "insert into indextest(tid,tname,tdate) values(" + tid
					+ ", 'test" + tid + "', 1000000" + tid + ")";
			Parser psr = new Parser(cmd);
			InsertData id = (InsertData) psr.updateCommand();
			new IndexUpdatePlanner().executeInsert(id, tx);
		}

		// Execute modification
		String cmd = "update indextest set tname = 'kkk', tdate=999999999 where tid > 1";
		Parser psr = new Parser(cmd);
		ModifyData md = (ModifyData) psr.updateCommand();
		int n = new IndexUpdatePlanner().executeModify(md, tx);
		assertTrue("*****IndexUpdatePlannerTest: bad modification", n > 0);

		// Check if the data has been modified
		String qry = "select tid, tname, tdate from indextest tid > 1";
		psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		Scan s = p.open();
		s.beforeFirst();
		boolean modi = true;
		while (s.next()) {
			if (!((String) s.getVal("tname").asJavaVal()).equals("kkk")
					&& (Long) s.getVal("tdate").asJavaVal() == 999999999L)
				modi = false;
		}
		s.close();
		assertEquals("*****IndexUpdatePlannerTest: wrong records modified",
				true, modi);
	}

	@Test
	public void testModifyOnSelectFld() {
		int insertCount = 0;
		// Insert the data that will be deleted
		for (int tid = 2; tid < 15; tid++) {
			String cmd = "insert into indextest(tid,tname,tdate) values(" + tid
					+ ", 'test" + tid + "', 1000000" + tid + ")";
			Parser psr = new Parser(cmd);
			InsertData id = (InsertData) psr.updateCommand();
			new IndexUpdatePlanner().executeInsert(id, tx);
			insertCount++;
		}

		// Execute modification
		String cmd = "update indextest set tid = 999, tname = 'kkk', tdate=999999999 where tid > 1 and tid < 15";
		Parser psr = new Parser(cmd);
		ModifyData md = (ModifyData) psr.updateCommand();
		int n = new IndexUpdatePlanner().executeModify(md, tx);

		assertTrue("*****IndexUpdatePlannerTest: bad modification",
				n == insertCount);

		// Check if the data has been modified
		String qry = "select tid, tname, tdate from indextest tid = 999";
		psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		Scan s = p.open();
		s.beforeFirst();
		int selectCount = 0;
		while (s.next()) {
			assertEquals("*****IndexUpdatePlannerTest: wrong records modified",
					(String) s.getVal("tname").asJavaVal(), "kkk");
			assertEquals("*****IndexUpdatePlannerTest: wrong records modified",
					(Long) s.getVal("tdate").asJavaVal(), (Long) 999999999L);
			assertEquals("*****IndexUpdatePlannerTest: wrong records modified",
					(Integer) s.getVal("tid").asJavaVal(), (Integer) 999);
			selectCount++;
		}
		s.close();
		assertTrue("*****IndexUpdatePlannerTest: bad modification",
				selectCount == insertCount);
	}
}
