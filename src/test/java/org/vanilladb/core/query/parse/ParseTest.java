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
package org.vanilladb.core.query.parse;

import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.RecordComparator.DIR_ASC;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;

public class ParseTest {
	private static Logger logger = Logger.getLogger(ParseTest.class.getName());
	
	@BeforeClass
	public static void init() {
		ServerInit.init(ParseTest.class);

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN PARSE TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH PARSE TEST");
	}

	@Test
	public void testParseInsert() {
		String qry = "INSERT INTO dept(did,dname) values(11, 'basketry')";

		Parser parser = new Parser(qry);
		InsertData id = (InsertData) parser.updateCommand();
		assertTrue("*****ParseTest: bad parsing insertion", id.tableName()
				.equals("dept"));

		Iterator<Constant> vals = id.vals().iterator();
		Iterator<String> flds = id.fields().iterator();
		assertTrue("*****ParseTest: bad parsing insertion",
				flds.next().equals("did"));
		assertTrue("*****ParseTest: bad parsing insertion",
				flds.next().equals("dname"));
		assertTrue("*****ParseTest: bad parsing insertion",
				vals.next().equals(new IntegerConstant(11)));
		assertTrue("*****ParseTest: bad parsing insertion",
				vals.next().equals(new VarcharConstant("basketry")));

	}

	@Test
	public void testParseSelect() {
		// also test that '_' can be keyword's character
		String qry = "select student_name, count(distinct score), avg(sname), "
				+ "sum(sid) from student, dept where sid=555 and sdid = did "
				+ "group by student_name order by sid asc";

		Parser parser = new Parser(qry);
		QueryData data = parser.queryCommand();
		Iterator<String> tbls = data.tables().iterator();
		assertTrue("*****ParseTest: bad parsing insertion",
				tbls.next().equals("student") && tbls.next().equals("dept"));

		assertTrue("*****ParseTest: bad parsing selection",
				data.projectFields().size() == 4
						&& data.projectFields().contains("student_name")
						&& data.projectFields().contains("dstcountofscore")
						&& data.projectFields().contains("avgofsname")
						&& data.projectFields().contains("sumofsid"));

		assertTrue("*****ParseTest: bad parsing selection", data
				.aggregationFn().size() == 3);

		Iterator<String> gbf = data.groupFields().iterator();
		assertTrue("*****ParseTest: bad parsing selection",
				gbf.next().equals("student_name"));

		Iterator<String> sbf = data.sortFields().iterator();
		assertTrue("*****ParseTest: bad parsing selection",
				sbf.next().equals("sid"));
		assertTrue("*****ParseTest: bad parsing selection", data
				.sortDirections().get(0) == DIR_ASC);

	}

	@Test
	public void testParseConstant() {
		String qry = "INSERT INTO lab(lid, lname, lbudget, lserial) "
				+ " values(11, 'netdb', 700.26, -1234567891025)";

		Parser parser = new Parser(qry);
		InsertData id = (InsertData) parser.updateCommand();

		Iterator<Constant> vals = id.vals().iterator();

		assertTrue("*****ParseTest: bad parsing constant",
				vals.next().equals(new IntegerConstant(11)));
		assertTrue("*****ParseTest: bad parsing insertion",
				vals.next().equals(new VarcharConstant("netdb")));
		assertTrue("*****ParseTest: bad parsing insertion",
				vals.next().equals(new DoubleConstant(700.26)));
		assertTrue("*****ParseTest: bad parsing insertion",
				vals.next().equals(new BigIntConstant(-1234567891025L)));

	}

	@Test
	public void testCreateTable() {
		/*
		 * New streamtokenizer can parse double value likes +234.11e-5; so we
		 * should check that it won't treat keyword starts with e/E as numeric
		 * value.
		 */
		String qry = "create table Enro11(Eid int, StudentId int, "
				+ "SectionId int, Grade varchar(2)) ";

		Parser parser = new Parser(qry);
		CreateTableData ctd = (CreateTableData) parser.updateCommand();
		SortedSet<String> fldSet = ctd.newSchema().fields();
		Iterator<String> flds = fldSet.iterator();
		assertTrue("*****ParseTest: bad parsing creat table", ctd.tableName()
				.equals("enro11"));
		assertTrue("*****ParseTest: bad parsing creat table", flds.next()
				.equals("eid"));
		assertTrue("*****ParseTest: bad parsing creat table", flds.next()
				.equals("grade"));
		assertTrue("*****ParseTest: bad parsing creat table", flds.next()
				.equals("sectionid"));
		assertTrue("*****ParseTest: bad parsing creat table", flds.next()
				.equals("studentid"));
	}
}
