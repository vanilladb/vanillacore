/* Copyright 2016-2021 vanilladb.org contributors*/
package org.vanilladb.core.storage.index.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

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
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
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

public class BTreeIndexTest {
  private static Logger logger = Logger.getLogger(BTreeIndexTest.class.getName());

  private static String FILE_PREFIX = "_test" + System.currentTimeMillis() + "_";
  private static final String DATA_TABLE_NAME = FILE_PREFIX + "BtreeData";

  private static CatalogMgr catMgr;

  private Transaction tx;

  @BeforeClass
  public static void init() {
    ServerInit.init(BTreeIndexTest.class);
    RecoveryMgr.enableLogging(false);
    catMgr = VanillaDb.catalogMgr();

    if (logger.isLoggable(Level.INFO)) logger.info("BEGIN BTREE INDEX TEST");

    Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
    Schema schema = new Schema();
    schema.addField("cid", INTEGER);
    schema.addField("title", VARCHAR(20));
    schema.addField("deptid", INTEGER);
    schema.addField("majorid", BIGINT);
    catMgr.createTable(DATA_TABLE_NAME, schema, tx);

    List<String> idxFlds1 = new LinkedList<String>();
    idxFlds1.add("cid");
    List<String> idxFlds2 = new LinkedList<String>();
    idxFlds2.add("title");
    List<String> idxFlds3 = new LinkedList<String>();
    idxFlds3.add("deptid");
    List<String> idxFlds4 = new LinkedList<String>();
    idxFlds4.add("majorid");

    catMgr.createIndex("_tempI1", DATA_TABLE_NAME, idxFlds1, IndexType.BTREE, tx);
    catMgr.createIndex("_tempI2", DATA_TABLE_NAME, idxFlds2, IndexType.BTREE, tx);
    catMgr.createIndex("_tempI3", DATA_TABLE_NAME, idxFlds3, IndexType.BTREE, tx);
    catMgr.createIndex("_tempI4", DATA_TABLE_NAME, idxFlds4, IndexType.BTREE, tx);

    List<String> idxFlds5 = new LinkedList<String>();
    idxFlds5.add("cid");
    idxFlds5.add("deptid");
    catMgr.createIndex("_tempH_MI1", DATA_TABLE_NAME, idxFlds5, IndexType.BTREE, tx);

    tx.commit();
  }

  @AfterClass
  public static void finish() {
    RecoveryMgr.enableLogging(true);

    if (logger.isLoggable(Level.INFO)) logger.info("FINISH BTREE INDEX TEST");
  }

  @Before
  public void createTx() {
    tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
  }

  @After
  public void finishTx() {
    tx.commit();
  }

  @Test
  public void testBasicOperation() {
    List<IndexInfo> idxList = catMgr.getIndexInfo(DATA_TABLE_NAME, "cid", tx);
    Index index = idxList.get(0).open(tx);

    // Insert 10 records with the same key
    RecordId[] records = new RecordId[10];
    BlockId blk = new BlockId(DATA_TABLE_NAME + ".tbl", 0);
    SearchKey int5 = new SearchKey(new IntegerConstant(5));
    for (int i = 0; i < 10; i++) {
      records[i] = new RecordId(blk, i);
      index.insert(int5, records[i], false);
    }

    // Insert a record with another key
    RecordId rid2 = new RecordId(blk, 9);
    SearchKey int7 = new SearchKey(new IntegerConstant(7));
    index.insert(int7, rid2, false);

    // It should find 10 records for int 5
    index.beforeFirst(new SearchRange(int5));
    int k = 0;
    while (index.next()) k++;
    Assert.assertEquals("*****BTreeIndexTest: bad insert", 10, k);

    // It should find only one record for int 7
    index.beforeFirst(new SearchRange(int7));
    index.next();
    assertTrue("*****BTreeIndexTest: bad read index", index.getDataRecordId().equals(rid2));

    // Delete the 10 records with key int 5
    for (int i = 0; i < 10; i++) index.delete(int5, records[i], false);
    index.beforeFirst(new SearchRange(int5));
    assertTrue("*****BTreeIndexTest: bad delete", index.next() == false);

    // Delete the record with key int 7
    index.delete(int7, rid2, false);
    index.close();
  }

  @Test
  public void testVarcharKey() {
    List<IndexInfo> idxList = catMgr.getIndexInfo(DATA_TABLE_NAME, "title", tx);
    Index index = idxList.get(0).open(tx);

    BlockId blk = new BlockId(DATA_TABLE_NAME + ".tbl", 0);

    int repeat = 1000;
    String str1 = "BAEBAEBAEBASAEBASE";
    String str3 = "AAEBAEBAEBASAEBASZ";
    String str4 = "BAEBAEBAEBASAEBASZ1";
    String str2 = "KARBAEBAEBASAEBASE";
    SearchKey key1 = new SearchKey(new VarcharConstant(str1, VARCHAR(20)));
    SearchKey key2 = new SearchKey(new VarcharConstant(str2, VARCHAR(20)));
    SearchKey key3 = new SearchKey(new VarcharConstant(str3, VARCHAR(20)));
    SearchKey key4 = new SearchKey(new VarcharConstant(str4, VARCHAR(20)));

    for (int i = 0; i < repeat; i++) {
      index.insert(key1, new RecordId(blk, i), false);
      index.insert(key2, new RecordId(blk, repeat + i), false);
      index.insert(key3, new RecordId(blk, repeat * 2 + i), false);
      index.insert(key4, new RecordId(blk, repeat * 3 + i), false);
    }

    index.beforeFirst(new SearchRange(key1));
    int j = 0;
    while (index.next()) j++;
    assertTrue("*****BTreeIndexTest: varchar selection", j == repeat);

    for (int i = 0; i < repeat; i++) {
      index.delete(key1, new RecordId(blk, i), false);
      index.delete(key2, new RecordId(blk, repeat + i), false);
      index.delete(key3, new RecordId(blk, repeat * 2 + i), false);
      index.delete(key4, new RecordId(blk, repeat * 3 + i), false);
    }

    index.close();
  }

  @Test
  public void testDir() {
    List<IndexInfo> idxList = catMgr.getIndexInfo(DATA_TABLE_NAME, "majorid", tx);
    Index index = idxList.get(0).open(tx);
    BlockId blk1 = new BlockId(DATA_TABLE_NAME + ".tbl", 0);
    int maxValue = 250; // 40000000
    /*
     * for 4K block, int value: repeat same val 250 times may create
     * overflow blk if repeat =200, the btree will create new node at k< 250
     * and k<250*250. bigint: 200 will overflow. make new node at <40000
     */
    int repeat = 170;
    for (int k = 0; k < maxValue; k++) {
      SearchKey key = new SearchKey(new BigIntConstant(k));
      for (int i = 0; i < repeat; i++) {
        index.insert(key, new RecordId(blk1, k * repeat + i), false);
      }
    }

    SearchKey int100 = new SearchKey(new IntegerConstant(100));
    SearchRange range100 = new SearchRange(int100);
    index.beforeFirst(range100);
    int j = 0;
    while (index.next()) j++;
    assertTrue("*****BTreeIndexTest: bad equal with", j == repeat);

    for (int i = 0; i < repeat; i++) {
      index.delete(int100, new RecordId(blk1, 100 * repeat + i), false);
    }
    index.beforeFirst(range100);
    assertTrue("*****BTreeIndexTest: bad delete", index.next() == false);
  }

  @Test
  public void testBTreeIndex() {
    List<IndexInfo> idxList = catMgr.getIndexInfo(DATA_TABLE_NAME, "deptid", tx);
    Index index = idxList.get(0).open(tx);
    BlockId blk0 = new BlockId(DATA_TABLE_NAME + ".tbl", 0);
    BlockId blk23 = new BlockId(DATA_TABLE_NAME + ".tbl", 23);
    int maxValue = 300;
    int repeat = 200;

    // Insert data set (maxValue x repeat)
    for (int fieldVal = 0; fieldVal < maxValue; fieldVal++) {
      for (int i = 0; i < repeat; i++) {
        SearchKey key = new SearchKey(new BigIntConstant(fieldVal));
        index.insert(key, new RecordId(blk0, fieldVal * repeat + i), false);
      }
    }

    // Insert 500 records with the same key (integer 7)
    SearchKey int7 = new SearchKey(new IntegerConstant(7));
    for (int count = 0; count < 500; count++)
      index.insert(int7, new RecordId(blk23, 2500 + count), false);

    // Search for the records each of which key is large than 50
    ConstantRange range = ConstantRange.newInstance(new IntegerConstant(50), false, null, false);
    index.beforeFirst(new SearchRange(range));
    int count = 0;
    while (index.next()) count++;
    assertEquals("*****BTreeIndexTest: bad > selection", (maxValue - 51) * repeat, count);

    Constant int5con = new IntegerConstant(5);
    // test less than
    range = ConstantRange.newInstance(null, false, int5con, false);
    index.beforeFirst(new SearchRange(range));
    count = 0;
    while (index.next()) count++;
    assertEquals("*****BTreeIndexTest: bad < selection", 5 * repeat, count);

    // test equality
    index.beforeFirst(new SearchRange(new SearchKey(int5con)));
    count = 0;
    while (index.next()) count++;
    assertEquals("*****BTreeIndexTest: bad equal with", repeat, count);

    // test delete
    for (int k = 0; k < maxValue; k++) {
      for (int i = 0; i < repeat; i++) {
        SearchKey key = new SearchKey(new BigIntConstant(k));
        index.delete(key, new RecordId(blk0, k * repeat + i), false);
      }
    }
    index.beforeFirst(new SearchRange(new SearchKey(int5con)));
    assertEquals("*****BTreeIndexTest: bad delete", false, index.next());

    count = 0;
    while (count < 500) {
      index.delete(int7, new RecordId(blk23, 2500 + count), false);
      count++;
    }
    index.beforeFirst(new SearchRange(int7));
    assertEquals("*****BTreeIndexTest: bad delete", false, index.next());

    index.close();
  }

  @Test
  public void testMultiKeys() {
    List<IndexInfo> idxList = catMgr.getIndexInfo(DATA_TABLE_NAME, "cid", tx);

    // Find the required index
    IndexInfo indexInfo = null;

    for (IndexInfo ii : idxList) {
      if (ii.fieldNames().contains("cid") && ii.fieldNames().contains("deptid")) indexInfo = ii;
    }

    if (indexInfo == null) Assert.fail("*****HashIndexTest: bad index metadata");

    Index index = indexInfo.open(tx);

    // Insert 10 records with the same keys
    BlockId blk = new BlockId(DATA_TABLE_NAME + ".tbl", 0);
    RecordId[] records1 = new RecordId[10];
    SearchKey key1_1 = new SearchKey(new IntegerConstant(1), new IntegerConstant(1));
    for (int i = 0; i < 10; i++) {
      records1[i] = new RecordId(blk, i);
      index.insert(key1_1, records1[i], false);
    }

    // Insert 1 records with another key
    blk = new BlockId(DATA_TABLE_NAME + ".tbl", 1);
    RecordId record2 = new RecordId(blk, 100);
    SearchKey key2_1 = new SearchKey(new IntegerConstant(2), new IntegerConstant(1));
    index.insert(key2_1, record2, false);

    // Insert 10 records with the third key
    blk = new BlockId(DATA_TABLE_NAME + ".tbl", 2);
    RecordId[] records3 = new RecordId[10];
    SearchKey key3_1 = new SearchKey(new IntegerConstant(3), new IntegerConstant(1));
    for (int i = 0; i < 10; i++) {
      records3[i] = new RecordId(blk, i);
      index.insert(key3_1, records3[i], false);
    }

    // It should find 10 records for the first key
    index.beforeFirst(new SearchRange(key1_1));
    int count = 0;
    while (index.next()) count++;
    assertTrue("*****HashIndexTest: bad insert", count == 10);

    // It should find only one record for the second key
    index.beforeFirst(new SearchRange(key2_1));
    index.next();
    assertTrue("*****HashIndexTest: bad read index", index.getDataRecordId().equals(record2));

    // It should find 10 records for the third key
    index.beforeFirst(new SearchRange(key3_1));
    count = 0;
    while (index.next()) count++;
    assertTrue("*****HashIndexTest: bad insert", count == 10);

    // Delete the records with the first key
    for (int i = 0; i < 10; i++) index.delete(key1_1, records1[i], false);
    index.beforeFirst(new SearchRange(key1_1));
    assertTrue("*****HashIndexTest: bad delete", index.next() == false);

    index.close();
  }
}
