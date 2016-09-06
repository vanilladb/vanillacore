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
package org.vanilladb.core.storage.index.hash;

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
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class HashIndexTest {
	private static Logger logger = Logger.getLogger(HashIndexTest.class
			.getName());
	private static CatalogMgr md;
	
	private static String FILE_PREFIX = "_test" + System.currentTimeMillis() + "_";
	private static String dataTableName = FILE_PREFIX + "HITable";
	
	private Transaction tx;

	@BeforeClass
	public static void init() {
		ServerInit.init(HashIndexTest.class);
		RecoveryMgr.enableLogging(false);
		md = VanillaDb.catalogMgr();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN HASH INDEX TEST");

		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Schema schema = new Schema();
		schema.addField("cid", INTEGER);
		schema.addField("title", VARCHAR(20));
		schema.addField("deptid", INTEGER);
		md.createTable(dataTableName, schema, tx);
		md.createIndex("_tempHI1", dataTableName, "cid", IDX_HASH, tx);
		md.createIndex("_tempHI2", dataTableName, "title", IDX_HASH, tx);
		md.createIndex("_tempHI3", dataTableName, "deptid", IDX_HASH, tx);

		tx.commit();
	}
	
	@AfterClass
	public static void finish() {
		RecoveryMgr.enableLogging(true);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH HASH INDEX TEST");
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
	public void testHashIndex() {
		Map<String, IndexInfo> idxmap = md.getIndexInfo(dataTableName, tx);
		Index cidIndex = idxmap.get("cid").open(tx);
		RecordId[] records = new RecordId[10];
		BlockId blk = new BlockId(dataTableName + ".tbl", 0);
		Constant int5 = new IntegerConstant(5);
		for (int i = 0; i < 10; i++) {
			records[i] = new RecordId(blk, i);
			cidIndex.insert(int5, records[i], false);
		}

		RecordId rid2 = new RecordId(blk, 9);
		Constant int7 = new IntegerConstant(7);
		cidIndex.insert(int7, rid2, false);

		cidIndex.beforeFirst(ConstantRange.newInstance(new IntegerConstant(5)));
		int k = 0;
		while (cidIndex.next())
			k++;
		assertTrue("*****HashIndexTest: bad insert", k == 10);

		cidIndex.beforeFirst(ConstantRange.newInstance(new IntegerConstant(7)));
		cidIndex.next();
		assertTrue("*****HashIndexTest: bad read index", cidIndex
				.getDataRecordId().equals(rid2));

		for (int i = 0; i < 10; i++)
			cidIndex.delete(int5, records[i], false);
		cidIndex.beforeFirst(ConstantRange.newInstance(new IntegerConstant(5)));
		assertTrue("*****HashIndexTest: bad delete", cidIndex.next() == false);

		cidIndex.delete(int7, rid2, false);
		cidIndex.close();
	}
}
