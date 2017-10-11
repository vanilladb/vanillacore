package org.vanilladb.core.query.algebra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.predicate.Term.OP_EQ;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
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

public class QueryTest {
	private static Logger logger = Logger.getLogger(QueryTest.class.getName());	
	private Transaction tx;
	
	@BeforeClass
	public static void init() {
		ServerInit.init(QueryTest.class);
		ServerInit.loadTestbed();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN QUERY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH QUERY TEST");
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
	public void testTable() {
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
	}

	@Test
	public void testSelect() {
		Plan p1 = new TablePlan("student", tx);
		Expression exp1 = new FieldNameExpression("majorid");
		Expression exp2 = new ConstantExpression(new IntegerConstant(30));
		Term t = new Term(exp1, OP_EQ, exp2);
		Predicate pred = new Predicate(t);
		Plan p2 = new SelectPlan(p1, pred);
		Scan s = p2.open();
		s.beforeFirst();
		while (s.next())
			assertEquals("*****QueryTest: bad select scan", (Integer) 30,
					(Integer) s.getVal("majorid").asJavaVal());
		s.close();
	}

	@Test
	public void testProject() {
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
	}

	@Test
	public void testProduct() {
		Plan p1 = new TablePlan("student", tx);
		Plan p2 = new TablePlan("dept", tx);
		Plan p3 = new ProductPlan(p1, p2);
		Expression exp1 = new FieldNameExpression("majorid");
		Expression exp2 = new FieldNameExpression("did");
		Predicate pred = new Predicate(new Term(exp1, OP_EQ, exp2));
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
	}
}
