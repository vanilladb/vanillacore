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
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchRange;
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
		
		List<String> idxFlds1 = new LinkedList<String>();
		idxFlds1.add("cid");
		List<String> idxFlds2 = new LinkedList<String>();
		idxFlds2.add("title");
		List<String> idxFlds3 = new LinkedList<String>();
		idxFlds3.add("deptid");
		
		md.createIndex("_tempHI1", dataTableName, idxFlds1, IndexType.HASH, tx);
		md.createIndex("_tempHI2", dataTableName, idxFlds2, IndexType.HASH, tx);
		md.createIndex("_tempHI3", dataTableName, idxFlds3, IndexType.HASH, tx);

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
		List<IndexInfo> idxList = md.getIndexInfo(dataTableName, "cid", tx);
		Index cidIndex = idxList.get(0).open(tx);
		
		// Insert 10 records with the same key
		RecordId[] records = new RecordId[10];
		BlockId blk = new BlockId(dataTableName + ".tbl", 0);
		SearchKey int5 = new SearchKey(new IntegerConstant(5));
		for (int i = 0; i < 10; i++) {
			records[i] = new RecordId(blk, i);
			cidIndex.insert(int5, records[i], false);
		}
		
		// Insert a record with another key
		RecordId rid2 = new RecordId(blk, 9);
		SearchKey int7 = new SearchKey(new IntegerConstant(7));
		cidIndex.insert(int7, rid2, false);
		
		// It should find 10 records for int 5
		cidIndex.beforeFirst(new SearchRange(int5));
		int k = 0;
		while (cidIndex.next())
			k++;
		assertTrue("*****HashIndexTest: bad insert", k == 10);
		
		// It should find only one record for int 7
		cidIndex.beforeFirst(new SearchRange(int7));
		cidIndex.next();
		assertTrue("*****HashIndexTest: bad read index", cidIndex
				.getDataRecordId().equals(rid2));
		
		// Delete the 10 records with key int 5
		for (int i = 0; i < 10; i++)
			cidIndex.delete(int5, records[i], false);
		cidIndex.beforeFirst(new SearchRange(int5));
		assertTrue("*****HashIndexTest: bad delete", cidIndex.next() == false);
		
		// Delete the record with key int 7
		cidIndex.delete(int7, rid2, false);
		cidIndex.close();
	}
}
