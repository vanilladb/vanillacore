/*******************************************************************************
 * Copyright 2016-2021 vanilladb.org contributors
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
package org.vanilladb.core.query.planner.opt;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.planner.Planner;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;

public class MultiKeyIndexPlanningTest {
  private static Logger logger = Logger.getLogger(MultiKeyIndexPlanningTest.class.getName());

  private static final String TABLE_NAME = "testing_table";
  private static final String INDEX_NAME = "testing_index";
  private static final String JOIN_TABLE_NAME = "testing_join_table";
  private static final int KEY_MAX = 20;

  @BeforeClass
  public static void init() {
    ServerInit.init(MultiKeyIndexPlanningTest.class);

    generateTestingData();

    if (logger.isLoggable(Level.INFO)) logger.info("BEGIN MULTI-KEY INDEXES QUERY TEST");
  }

  @AfterClass
  public static void finish() {
    if (logger.isLoggable(Level.INFO)) logger.info("FINISH MULTI-KEY INDEXES QUERY TEST");
  }

  private static void generateTestingData() {
    if (logger.isLoggable(Level.INFO)) logger.info("loading data");

    CatalogMgr cataMgr = VanillaDb.catalogMgr();
    Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);

    // Create a table
    Schema sch = new Schema();
    sch.addField("key_1", Type.INTEGER);
    sch.addField("key_2", Type.INTEGER);
    sch.addField("key_3", Type.INTEGER);
    sch.addField("data", Type.VARCHAR(100));
    cataMgr.createTable(TABLE_NAME, sch, tx);

    // Create a multi-key index
    List<String> indexedFlds = new LinkedList<String>();
    indexedFlds.add("key_1");
    indexedFlds.add("key_2");
    indexedFlds.add("key_3");
    cataMgr.createIndex(INDEX_NAME, TABLE_NAME, indexedFlds, IndexType.BTREE, tx);

    // Load data
    TableInfo ti = cataMgr.getTableInfo(TABLE_NAME, tx);
    IndexInfo ii = cataMgr.getIndexInfoByName(INDEX_NAME, tx);
    RecordFile rf = ti.open(tx, true);
    Index idx = ii.open(tx);

    for (int key1 = 1; key1 <= KEY_MAX; key1++) {
      for (int key2 = 1; key2 <= KEY_MAX; key2++) {
        for (int key3 = 1; key3 <= KEY_MAX; key3++) {

          // Insert a record
          rf.insert();
          rf.setVal("key_1", new IntegerConstant(key1));
          rf.setVal("key_2", new IntegerConstant(key2));
          rf.setVal("key_3", new IntegerConstant(key3));
          rf.setVal("data", toTestingData(key1, key2, key3));

          // Insert an index record
          SearchKey key =
              new SearchKey(
                  new IntegerConstant(key1), new IntegerConstant(key2), new IntegerConstant(key3));
          idx.insert(key, rf.currentRecordId(), true);
        }
      }
    }

    // Finish
    rf.close();
    idx.close();

    // Create a join table
    sch = new Schema();
    sch.addField("join_key_1", Type.INTEGER);
    sch.addField("join_key_2", Type.INTEGER);
    sch.addField("join_key_3", Type.INTEGER);
    sch.addField("join_data", Type.VARCHAR(100));
    cataMgr.createTable(JOIN_TABLE_NAME, sch, tx);

    // Load data
    ti = cataMgr.getTableInfo(JOIN_TABLE_NAME, tx);
    rf = ti.open(tx, true);

    for (int key1 = 1; key1 <= KEY_MAX; key1++) {
      for (int key2 = 1; key2 <= KEY_MAX; key2++) {
        for (int key3 = 1; key3 <= KEY_MAX; key3++) {
          // Insert a record
          rf.insert();
          rf.setVal("join_key_1", new IntegerConstant(key1));
          rf.setVal("join_key_2", new IntegerConstant(key2));
          rf.setVal("join_key_3", new IntegerConstant(key3));
          rf.setVal("join_data", toTestingJoinData(key1, key2, key3));
        }
      }
    }

    // Finish
    rf.close();

    tx.commit();
  }

  private static VarcharConstant toTestingData(int key1, int key2, int key3) {
    return new VarcharConstant(String.format("test_%d_%d_%d", key1, key2, key3));
  }

  private static VarcharConstant toTestingJoinData(int key1, int key2, int key3) {
    return new VarcharConstant(String.format("join_test_%d_%d_%d", key1, key2, key3));
  }

  private Transaction tx;

  @Before
  public void createTx() {
    tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
  }

  @After
  public void finishTx() {
    tx.commit();
  }

  @Test
  public void testMultiKeysSelection() {
    String sql =
        "SELECT data FROM " + TABLE_NAME + " WHERE key_1 = 1 AND " + "key_2 = 2 AND key_3 = 3";
    Planner planer = VanillaDb.newPlanner();

    Plan p = planer.createQueryPlan(sql, tx);

    // Check the explain string
    String explain = p.toString();

    String[] lines = explain.split("\n");
    String idxSecPlanLine = null;
    for (String line : lines) {
      if (line.contains("IndexSelectPlan")) {
        idxSecPlanLine = line;
        break;
      }
    }

    assertTrue("*****MultiKeyIndexPlanningTest: bad planning", idxSecPlanLine != null);
    assertTrue("*****MultiKeyIndexPlanningTest: bad planning", idxSecPlanLine.contains("key_1"));
    assertTrue("*****MultiKeyIndexPlanningTest: bad planning", idxSecPlanLine.contains("key_2"));
    assertTrue("*****MultiKeyIndexPlanningTest: bad planning", idxSecPlanLine.contains("key_3"));

    // Check the result
    Scan scan = p.open();

    scan.beforeFirst();

    if (!scan.next()) Assert.fail("*****MultiKeyIndexPlanningTest: could not find the record");

    Assert.assertEquals(toTestingData(1, 2, 3), scan.getVal("data"));

    if (scan.next()) Assert.fail("*****MultiKeyIndexPlanningTest: wrong count");

    scan.close();
  }

  /** TODO: Figure out how to activate IndexJoinPlan */
  //	@Test
  //	public void testMultiKeysJoin() {
  //		String sql = String.format("SELECT data, join_data FROM %s, %s WHERE "
  //				+ "key_1 = 1 AND key_2 = 2 AND key_3 = 3 AND join_key_1 = key_1 "
  //				+ "AND join_key_2 = key_2 AND join_key_3 = key_3", TABLE_NAME,
  //				JOIN_TABLE_NAME);
  //		Planner planer = VanillaDb.newPlanner();
  //
  //		Plan p = planer.createQueryPlan(sql, tx);
  //
  //		// Check the explain string
  //		String explain = p.toString();
  //		System.out.println(explain);
  //	}
}
