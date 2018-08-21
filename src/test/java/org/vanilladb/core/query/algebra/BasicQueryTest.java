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
package org.vanilladb.core.query.algebra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.predicate.ConstantExpression;
import org.vanilladb.core.sql.predicate.Expression;
import org.vanilladb.core.sql.predicate.FieldNameExpression;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term;
import org.vanilladb.core.storage.tx.Transaction;

public class BasicQueryTest {
	private static Logger logger = Logger.getLogger(BasicQueryTest.class.getName());
	
	@BeforeClass
	public static void init() {
		ServerInit.init(BasicQueryTest.class);
		ServerInit.loadTestbed();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN BASIC QUERY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH BASIC QUERY TEST");
	}
	
	@Test
	public void testTable() {
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		Plan p = new TablePlan("student", tx);
		Scan s = p.open();
		int id = 0;
		s.beforeFirst();
		while (s.next()) {
			assertTrue(
					"*****QueryTest: bad table scan",
					s.getVal("sid").equals(new IntegerConstant(id))
							&& s.getVal("sname").asJavaVal()
									.equals("student" + id)
							&& s.getVal("gradyear").equals(
									new IntegerConstant(id % 50 + 1960)));
			id++;
		}
		s.close();
		tx.commit();
	}

	@Test
	public void testSelect() {
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		Plan p1 = new TablePlan("student", tx);
		Expression exp1 = new FieldNameExpression("majorid");
		Expression exp2 = new ConstantExpression(new IntegerConstant(30));
		Term t = new Term(exp1, Term.OP_EQ, exp2);
		Predicate pred = new Predicate(t);
		Plan p2 = new SelectPlan(p1, pred);
		Scan s = p2.open();
		s.beforeFirst();
		while (s.next())
			assertEquals("*****QueryTest: bad select scan", (Integer) 30,
					(Integer) s.getVal("majorid").asJavaVal());
		s.close();
		tx.commit();
	}

	@Test
	public void testProject() {
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		Plan p1 = new TablePlan("student", tx);
		Scan s1 = p1.open();
		s1.beforeFirst();
		int count1 = 0;
		while (s1.next())
			count1++;
		s1.close();

		Set<String> fields = new HashSet<String>(Arrays.asList("sname",
				"gradyear"));
		Plan p2 = new ProjectPlan(p1, fields);
		Scan s2 = p2.open();
		s2.beforeFirst();
		int count2 = 0;
		while (s2.next())
			count2++;
		s2.close();
		Schema sch = p2.schema();
		assertTrue(
				"*****QueryTest: bad project scan",
				count1 == count2 && sch.fields().size() == 2
						&& sch.hasField("sname") && sch.hasField("gradyear")
						&& !sch.hasField("sid") && !sch.hasField("majorid"));
		tx.commit();
	}

	@Test
	public void testProduct() {
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		Plan p1 = new TablePlan("student", tx);
		Plan p2 = new TablePlan("dept", tx);
		Plan p3 = new ProductPlan(p1, p2);
		Expression exp1 = new FieldNameExpression("majorid");
		Expression exp2 = new FieldNameExpression("did");
		Predicate pred = new Predicate(new Term(exp1, Term.OP_EQ, exp2));
		Plan p4 = new SelectPlan(p3, pred);
		Scan s1 = p1.open();
		s1.beforeFirst();
		int count1 = 0;
		while (s1.next())
			count1++;
		s1.close();
		Scan s2 = p2.open();
		s2.beforeFirst();
		int count2 = 0;
		while (s2.next())
			count2++;
		s2.close();
		Scan s3 = p3.open();
		s3.beforeFirst();
		int count3 = 0;
		while (s3.next())
			count3++;
		s3.close();
		Scan s4 = p4.open();
		s4.beforeFirst();
		int count4 = 0;
		while (s4.next())
			count4++;
		s4.close();
		assertTrue("*****QueryTest: bad product scan", count3 == count1
				* count2
				&& count4 == count1);
		tx.commit();
	}
}
