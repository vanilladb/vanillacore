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
package org.vanilladb.core.query.planner.opt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

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
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.query.planner.BasicUpdatePlanner;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;

public class HeuristicQueryPlannerTest {
	private static Logger logger = Logger.getLogger(HeuristicQueryPlannerTest.class.getName());
	
	@BeforeClass
	public static void init() {
		ServerInit.init(HeuristicQueryPlannerTest.class);
		ServerInit.loadTestbed();
		
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
		tx.commit();
	}
	
	@Test
	public void testQuery() {
		String qry = "select sid, sname, majorid from student, dept "
				+ "where majorid=did and dname='dept0'";
		Parser psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		Schema sch = p.schema();
		assertTrue(
				"*****HeuristicQueryPlannerTest: bad heuristic plan schema",
				sch.fields().size() == 3 && sch.hasField("sid")
						&& sch.hasField("sname") && sch.hasField("majorid"));
		Scan s = p.open();
		s.beforeFirst();
		while (s.next())
			assertEquals(
					"*****HeuristicQueryPlannerTest: bad heuristic plan selection",
					(Integer) 0, (Integer) s.getVal("majorid").asJavaVal());
		s.close();
	}

	@Test
	public void testView() {
		String viewDef = "select sid, sname, majorid from student, dept "
				+ "where majorid=did and dname='dept0'";
		String cmd = "create view view02 as " + viewDef;
		Parser psr = new Parser(cmd);
		CreateViewData cvd = (CreateViewData) psr.updateCommand();
		int i = new BasicUpdatePlanner().executeCreateView(cvd, tx);
		assertTrue("*****HeuristicQueryPlannerTest: bad create view", i == 0);

		String qry = "select sid, sname, majorid from view02";
		psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		Schema sch = p.schema();
		assertTrue(
				"*****HeuristicQueryPlannerTest: bad view schema",
				sch.fields().size() == 3 && sch.hasField("sid")
						&& sch.hasField("sname") && sch.hasField("majorid"));
		Scan s = p.open();
		s.beforeFirst();
		while (s.next())
			assertEquals(
					"*****HeuristicQueryPlannerTest: bad basic plan selection from view",
					(Integer) 0, (Integer) s.getVal("majorid").asJavaVal());
		s.close();
	}

	@Test
	public void testJoinQuery() {
		// initial data
		CatalogMgr md = VanillaDb.catalogMgr();
		
		Schema sch = new Schema();
		sch.addField("aid", INTEGER);
		// ach.addStringField("aname", 20);
		sch.addField("acid", BIGINT);
		md.createTable("atable", sch, tx);
		
		TableInfo ti = md.getTableInfo("atable", tx);
		RecordFile rf = ti.open(tx, true);
		for (int id = 1; id < 9; id++) {
			rf.insert();
			rf.setVal("aid", new IntegerConstant(id));
			// rf.setString("title", "course" + id);
			if (id < 5)
				rf.setVal("acid", new BigIntConstant(10));
			else
				rf.setVal("acid", new BigIntConstant(20));
		}
		rf.close();

		sch = new Schema();
		sch.addField("cid", INTEGER);
		sch.addField("cname", VARCHAR(20));
		sch.addField("ctid", BIGINT);
		md.createTable("ctable", sch, tx);
		
		ti = md.getTableInfo("ctable", tx);
		rf = ti.open(tx, true);
		rf.insert();
		rf.setVal("cid", new IntegerConstant(10));
		rf.setVal("cname", new VarcharConstant("course10"));
		rf.setVal("ctid", new BigIntConstant(7));
		rf.insert();
		rf.setVal("cid", new IntegerConstant(20));
		rf.setVal("cname", new VarcharConstant("course20"));
		rf.setVal("ctid", new BigIntConstant(9));
		rf.insert();
		rf.setVal("cid", new IntegerConstant(30));
		rf.setVal("cname", new VarcharConstant("course30"));
		rf.setVal("ctid", new BigIntConstant(7));
		rf.close();

		sch = new Schema();
		sch.addField("tid", BIGINT);
		sch.addField("tname", VARCHAR(20));
		md.createTable("ttable", sch, tx);
		
		ti = md.getTableInfo("ttable", tx);
		rf = ti.open(tx, true);
		rf.insert();
		rf.setVal("tid", new BigIntConstant(7));
		rf.setVal("tname", new VarcharConstant("teacher7"));
		rf.insert();
		rf.setVal("tid", new BigIntConstant(9));
		rf.setVal("tname", new VarcharConstant("teacher9"));
		rf.insert();
		rf.setVal("tid", new BigIntConstant(30));
		rf.setVal("tname", new VarcharConstant("teacher30"));
		rf.close();

		String qry = "SELECT ctid, tname, aid, cname FROM atable, ctable, ttable "
				+ "WHERE acid = cid AND ctid = tid AND tid > 7";
		// order by sid desc group by sid
		Parser psr = new Parser(qry);
		QueryData qd = psr.queryCommand();

		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		sch = p.schema();

		assertTrue(
				"*****HeuristicQueryPlannerTest: bad heuristic plan schema",
				sch.fields().size() == 4 && sch.hasField("aid")
						&& sch.hasField("ctid") && sch.hasField("tname")
						&& sch.hasField("cname"));

		Scan s = p.open();
		s.beforeFirst();
		while (s.next())
			assertTrue("*****HeuristicQueryPlannerTest: bad heuristic query result",
					((Long) s.getVal("ctid").asJavaVal()) > 7);

		s.close();
	}
}
