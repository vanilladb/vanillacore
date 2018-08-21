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
package org.vanilladb.core.query.planner;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;

public class VerifierTest {
	private static Logger logger = Logger.getLogger(VerifierTest.class
			.getName());
	private Transaction tx;
	
	@BeforeClass
	public static void init() {
		ServerInit.init(VerifierTest.class);
		ServerInit.loadTestbed();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN QUERY VERIFIER TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH QUERY VERIFIER TEST");
	}
	
	
	@Before
	public void createTx() {
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
	}
	
	@After
	public void finishTx() {
		tx = null;
	}

	@Test
	public void testInsertData() {
		// test nonexistent table name
		try {
			String qry = "insert into non (sid, sname, majorid, gradyear) values (6, 'kay', 21, 2000)";
			Parser psr = new Parser(qry);
			InsertData data = (InsertData) psr.updateCommand();
			Verifier.verifyInsertData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}

		// test nonexistent field name
		try {
			String qry = "insert into student (sid, sname, non) values (6, 'kay', 21, 2000)";
			Parser psr = new Parser(qry);
			InsertData data = (InsertData) psr.updateCommand();
			Verifier.verifyInsertData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}

		// test non-valid value
		try {
			String qry = "insert into student (sid, sname, majorid, gradyear) values (6, 'kay', '1', 2000)";
			Parser psr = new Parser(qry);
			InsertData data = (InsertData) psr.updateCommand();
			Verifier.verifyInsertData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}

	}

	@Test
	public void testModifyData() {
		// test nonexistent table name
		try {
			String qry = "update notexisted set gradyear = add(gradyear, 1) where sid = sid";
			Parser psr = new Parser(qry);
			ModifyData data = (ModifyData) psr.updateCommand();
			Verifier.verifyModifyData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}

		// test nonexistent field name
		try {
			String qry = "update student set gradyear=add(notexisted, 1) where sid=sid";
			Parser psr = new Parser(qry);
			ModifyData data = (ModifyData) psr.updateCommand();
			Verifier.verifyModifyData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}
	}

	@Test
	public void testDeleteData() {
		// test nonexistent table name
		try {
			String qry = "delete from notexisted where sid=sid";
			Parser psr = new Parser(qry);
			DeleteData data = (DeleteData) psr.updateCommand();
			Verifier.verifyDeleteData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}
	}

	@Test
	public void testCreateTableData() {
		// test existent table name
		try {
			String qry = "create table student (pid int, pname varchar(20))";
			Parser psr = new Parser(qry);
			CreateTableData data = (CreateTableData) psr.updateCommand();
			Verifier.verifyCreateTableData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}
	}

	@Test
	public void testCreateIndexData() {
		// test nonexistent table name
		try {
			String qry = "create index idx_student on nonexisted(gradyear)";
			Parser psr = new Parser(qry);
			CreateIndexData data = (CreateIndexData) psr.updateCommand();
			Verifier.verifyCreateIndexData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}

		// test nonexistent field name
		try {
			String qry = "create index idx_student on student(notexisted)";
			Parser psr = new Parser(qry);
			CreateIndexData data = (CreateIndexData) psr.updateCommand();
			Verifier.verifyCreateIndexData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}
	}

	@Test
	public void testCreateViewData() {
		// test existent view name
		try {
			String qry = "create view student as select sname, dname from student, dept";
			VanillaDb.newPlanner().executeUpdate(qry, tx);

			Parser psr = new Parser(qry);
			CreateViewData data = (CreateViewData) psr.updateCommand();
			Verifier.verifyCreateViewData(data, tx);
			tx.commit();
			fail("QueryVerifierTest: bad verification");
		} catch (BadSemanticException e) {
			tx.rollback();
		}
		// test non-valid view definition
		try {
			String qry = "create view notexisted as select abc, efg from student, dept";
			Parser psr = new Parser(qry);
			CreateViewData data = (CreateViewData) psr.updateCommand();
			Verifier.verifyCreateViewData(data, tx);
			tx.commit();
			fail("QueryVerifierTest: bad verification");
		} catch (BadSemanticException e) {
			tx.rollback();
		}
	}
}
