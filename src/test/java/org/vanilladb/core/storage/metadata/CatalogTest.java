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
package org.vanilladb.core.storage.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.index.Index.IDX_HASH;

import java.sql.Connection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class CatalogTest {
	private static Logger logger = Logger
			.getLogger(CatalogTest.class.getName());

	private static CatalogMgr catMgr;
	private static Transaction tx;
	
	private static String FILE_PREFIX = "_test" + System.currentTimeMillis() + "_";

	@BeforeClass
	public static void init() {
		ServerInit.init(CatalogTest.class);

		catMgr = VanillaDb.catalogMgr();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN CATALOG TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH CATALOG TEST");
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
	public void testTableMgr() {
		// Create two tables
		Schema sch1 = new Schema();
		sch1.addField("A", INTEGER);
		sch1.addField("B", VARCHAR(30));
		Schema sch2 = new Schema();
		sch2.addField("A", INTEGER);
		sch2.addField("C", VARCHAR(10));
		String t1 = FILE_PREFIX + "T1";
		String t2 = FILE_PREFIX + "T2";
		catMgr.createTable(t1, sch1, tx);
		catMgr.createTable(t2, sch2, tx);
		
		// Get table infos
		TableInfo ti1 = catMgr.getTableInfo(t1, tx);
		TableInfo ti2 = catMgr.getTableInfo(t2, tx);
		TableInfo ti3 = catMgr.getTableInfo("T3", tx);
		
		// Assertion
		assertTrue("*****CatalogTest: bad table info", ti1.schema().fields()
				.size() == 2
				&& ti1.schema().hasField("A")
				&& ti1.schema().hasField("B")
				&& !ti1.schema().hasField("C"));
		assertTrue("*****CatalogTest: bad table info", ti2.schema().fields()
				.size() == 2
				&& ti2.schema().hasField("A")
				&& ti2.schema().hasField("C")
				&& !ti2.schema().hasField("B"));
		assertTrue("*****CatalogTest: bad table info", ti3 == null);
	}

	@Test
	public void testViewMgr() {
		String v1 = FILE_PREFIX + "V1";
		String v2 = FILE_PREFIX + "V2";
		String v3 = FILE_PREFIX + "V3";
		
		// Create fake views
		catMgr.createView(v1, "abcde", tx);
		catMgr.createView(v2, "select * from T", tx);
		
		// Retrieve fake views and inexistent views
		String s1 = catMgr.getViewDef(v1, tx);
		String s2 = catMgr.getViewDef(v2, tx);
		String s3 = catMgr.getViewDef(v3, tx);
		assertEquals("*****CatalogTest: bad view info", "abcde", s1);
		assertEquals("*****CatalogTest: bad view info", "select * from T", s2);
		assertNull("*****CatalogTest: bad view info", s3);
	}

	@Test
	public void testIndexMgr() {
		String tbl = FILE_PREFIX + "IdxTest";
		String i1 = FILE_PREFIX + "I1";
		String i2 = FILE_PREFIX + "I2";
		String i3 = FILE_PREFIX + "I3";
		
		// Create a table and three hash indexes
		Schema sch = new Schema();
		sch.addField("A", INTEGER);
		sch.addField("B", VARCHAR(20));
		sch.addField("C", INTEGER);
		catMgr.createTable(tbl, sch, tx);
		catMgr.createIndex(i1, tbl, "A", IDX_HASH, tx);
		catMgr.createIndex(i2, tbl, "B", IDX_HASH, tx);
		catMgr.createIndex(i3, tbl, "C", IDX_HASH, tx);
		
		// Check the existence of created indexes
		Map<String, IndexInfo> idxmap = catMgr.getIndexInfo(tbl, tx);
		assertTrue("*****CatalogTest: bad index info", idxmap.containsKey("A")
				&& idxmap.containsKey("B") && idxmap.containsKey("C")
				&& idxmap.keySet().size() == 3);

		// check for index open success and properties setting
		Index k = idxmap.get("A").open(tx);
		assertTrue("*****CatalogTest: bad index open", k != null);
	}
}
