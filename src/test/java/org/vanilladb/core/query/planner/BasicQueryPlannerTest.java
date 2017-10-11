/*******************************************************************************
 * Copyright 2017 vanilladb.org
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
package org.vanilladb.core.query.planner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.tx.Transaction;

public class BasicQueryPlannerTest {
	private static Logger logger = Logger
			.getLogger(BasicQueryPlannerTest.class.getName());
	private Transaction tx;
	
	@BeforeClass
	public static void init() {
		ServerInit.init(BasicQueryPlannerTest.class);
		ServerInit.loadTestbed();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN PLANNER TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH PLANNER TEST");
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
	public void testQuery() {
		String qry = "select sid, sname, majorid from student, dept "
				+ "where majorid=did and dname='dept0'";
		Parser psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new BasicQueryPlanner().createPlan(qd, tx);
		Schema sch = p.schema();
		assertTrue("*****PlannerTest: bad basic plan schema", sch.fields()
				.size() == 3
				&& sch.hasField("sid")
				&& sch.hasField("sname")
				&& sch.hasField("majorid"));
		Scan s = p.open();
		s.beforeFirst();
		while (s.next())
			assertEquals("*****PlannerTest: bad basic plan selection",
					(Integer) 0, (Integer) s.getVal("majorid").asJavaVal());
		s.close();
	}

	@Test
	public void testView() {
		String viewDef = "select sid, sname, majorid from student, dept "
				+ "where majorid=did and dname='dept0'";
		String cmd = "create view view01 as " + viewDef;
		Parser psr = new Parser(cmd);
		CreateViewData cvd = (CreateViewData) psr.updateCommand();
		int i = new BasicUpdatePlanner().executeCreateView(cvd, tx);
		assertTrue("*****PlannerTest: bad create view", i == 0);

		String qry = "select sid, sname, majorid from view01";
		psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new BasicQueryPlanner().createPlan(qd, tx);
		Schema sch = p.schema();
		assertTrue(
				"*****PlannerTest: bad view schema",
				sch.fields().size() == 3 && sch.hasField("sid")
						&& sch.hasField("sname") && sch.hasField("majorid"));
		Scan s = p.open();
		s.beforeFirst();
		while (s.next())
			assertEquals(
					"*****PlannerTest: bad basic plan selection from view",
					(Integer) 0, (Integer) s.getVal("majorid").asJavaVal());
		s.close();
	}

	@Test
	public void testInsert() {
		String qry = "select did from dept";
		Parser psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new BasicQueryPlanner().createPlan(qd, tx);

		Scan s = p.open();
		s.beforeFirst();
		int precount = 0;
		while (s.next())
			precount++;
		s.close();

		String cmd = "insert into dept(did,dname) values(-1, 'basketry')";
		psr = new Parser(cmd);
		InsertData id = (InsertData) psr.updateCommand();
		int n = new BasicUpdatePlanner().executeInsert(id, tx);
		if (n != 1)
			assertEquals("*****PlannerTest: bad insertion return value", 1, n);

		s = p.open();
		s.beforeFirst();
		int postcount = 0;
		while (s.next())
			postcount++;
		s.close();
		assertEquals("*****PlannerTest: bad insertion count", precount + 1,
				postcount);

		qry = "select did from dept where dname='basketry'";
		psr = new Parser(qry);
		qd = psr.queryCommand();
		p = new BasicQueryPlanner().createPlan(qd, tx);
		s = p.open();
		s.beforeFirst();
		int selectcount = 0;
		while (s.next()) {
			int i = (Integer) s.getVal("did").asJavaVal();
			assertEquals("*****PlannerTest: bad insert retrieval", -1, i);
			selectcount++;
		}
		assertEquals("*****PlannerTest: bad insert count", 1, selectcount);
	}

	@Test
	public void testDelete() {
		String cmd = "delete from student where majorid = 10";
		Parser psr = new Parser(cmd);
		DeleteData dd = (DeleteData) psr.updateCommand();
		BasicUpdatePlanner bp = new BasicUpdatePlanner();
		bp.executeDelete(dd, tx);

		String qry = "select sid from student where majorid = 10";
		psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new BasicQueryPlanner().createPlan(qd, tx);
		Scan s = p.open();
		s.beforeFirst();
		while (s.next())
			fail("*****PlannerTest: bad delete");
		s.close();
	}

	@Test
	public void testModify() {
		String cmd = "update student set majorid = -1, sname = 'kkkkk',  where majorid = 10";
		Parser psr = new Parser(cmd);
		ModifyData md = (ModifyData) psr.updateCommand();
		int n = new BasicUpdatePlanner().executeModify(md, tx);
		String qry = "select sid, sname from student where majorid = -1";
		psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new BasicQueryPlanner().createPlan(qd, tx);
		Scan s = p.open();
		s.beforeFirst();
		int count = 0;
		while (s.next()) {
			count++;
			assertEquals("*****PlannerTest: wrong records modified", (String) s
					.getVal("sname").asJavaVal(), "kkkkk");
		}
		s.close();
		assertEquals("*****PlannerTest: wrong records modified", n, count);

		qry = "select sid from student where majorid = 10";
		psr = new Parser(qry);
		qd = psr.queryCommand();
		p = new BasicQueryPlanner().createPlan(qd, tx);
		s = p.open();
		s.beforeFirst();
		while (s.next())
			fail("*****PlannerTest: not all records modified");
		s.close();
	}
}
