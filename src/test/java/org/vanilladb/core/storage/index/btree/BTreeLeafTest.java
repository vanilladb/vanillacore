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
package org.vanilladb.core.storage.index.btree;

import java.sql.Connection;
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
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class BTreeLeafTest {
  private static Logger logger = Logger.getLogger(BTreeLeafTest.class.getName());

  // the file starting with "_temp" will be deleted during initialization
  private static String FILE_PREFIX = "_test" + System.currentTimeMillis() + "_";
  private static final String INDEX_FILE_NAME = BTreeLeaf.getFileName(FILE_PREFIX + "BtreeLeaf");
  private static final BlockId DATA_BLOCK = new BlockId("_tempBtreeLeaf.tbl", 0);
  private static final SearchKeyType KEY_TYPE = new SearchKeyType(new Type[] {Type.INTEGER});
  private static final int MAX_NUM_OF_RECORDS;
  private static final int PRE_FORMATED_PAGE_COUNT = 4;

  static {
    int slotSize = BTreePage.slotSize(BTreeLeaf.schema(KEY_TYPE));
    int flagSize = BTreeLeaf.NUM_FLAGS * Type.INTEGER.maxSize();
    MAX_NUM_OF_RECORDS = (Buffer.BUFFER_SIZE - flagSize) / slotSize;
  }

  private Transaction tx;

  @BeforeClass
  public static void init() {
    ServerInit.init(BTreeLeafTest.class);
    RecoveryMgr.enableLogging(false);

    if (logger.isLoggable(Level.INFO)) logger.info("BEGIN BTREE LEAF TEST");

    Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);

    // Format the pages that will be used later
    for (int i = 0; i < PRE_FORMATED_PAGE_COUNT; i++)
      tx.bufferMgr()
          .pinNew(
              INDEX_FILE_NAME,
              new BTPageFormatter(BTreeLeaf.schema(KEY_TYPE), new long[] {-1, -1}));

    tx.commit();
  }

  @AfterClass
  public static void finish() {
    RecoveryMgr.enableLogging(true);

    if (logger.isLoggable(Level.INFO)) logger.info("FINISH BTREE LEAF TEST");
  }

  private static SearchRange newSearchRange(int integer) {
    return new SearchRange(
        new ConstantRange[] {ConstantRange.newInstance(new IntegerConstant(integer))});
  }

  private static SearchRange newSearchRange(int start, int end) {
    return new SearchRange(
        new ConstantRange[] {
          ConstantRange.newInstance(
              new IntegerConstant(start), true, new IntegerConstant(end), true)
        });
  }

  @Before
  public void createTx() {
    tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
  }

  @After
  public void finishTx() {
    tx.commit();
    tx = null;
  }

  @Test
  public void testInsert() {
    BlockId blk = new BlockId(INDEX_FILE_NAME, 0);
    BTreeLeaf leaf;
    SearchRange insertRange;
    int numOfRecords = 20;

    // Insert records
    for (int i = 0; i < numOfRecords; i++) {
      insertRange = newSearchRange(i);
      leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, insertRange, tx);
      leaf.insert(new RecordId(DATA_BLOCK, i));
      leaf.close();
    }

    // Check number of records
    SearchRange searchRange = newSearchRange(0, numOfRecords - 1);
    leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, searchRange, tx);
    Assert.assertEquals(numOfRecords, leaf.getNumRecords());

    // Check the order of records
    for (int i = 0; i < numOfRecords; i++) {
      if (!leaf.next()) Assert.fail("BTreeLeafTest: Bad leaf.next()");

      Assert.assertEquals(new RecordId(DATA_BLOCK, i), leaf.getDataRecordId());
    }
  }

  @Test
  public void testDelete() {
    BlockId blk = new BlockId(INDEX_FILE_NAME, 1);
    BTreeLeaf leaf;
    SearchRange insertRange;
    int numOfRecords = 20;

    // Insert records
    for (int i = 0; i < numOfRecords; i++) {
      insertRange = newSearchRange(i);
      leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, insertRange, tx);
      leaf.insert(new RecordId(DATA_BLOCK, i));
      leaf.close();
    }

    // Delete all records
    for (int i = 0; i < numOfRecords; i++) {
      SearchRange deleteRange = newSearchRange(i);
      leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, deleteRange, tx);
      leaf.delete(new RecordId(DATA_BLOCK, i));
      leaf.close();
    }

    // Check number of records
    SearchRange searchRange = newSearchRange(0, numOfRecords - 1);
    leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, searchRange, tx);
    Assert.assertEquals(0, leaf.getNumRecords());
  }

  @Test(timeout = 1000)
  public void testOverflow() {
    BlockId blk = new BlockId(INDEX_FILE_NAME, 2);
    BTreeLeaf leaf;
    SearchRange insertRange = newSearchRange(0);

    // Insert a lot of records with the same key
    int numOfRecords = MAX_NUM_OF_RECORDS * 3 / 2;
    for (int i = 0; i < numOfRecords; i++) {
      leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, insertRange, tx);
      leaf.insert(new RecordId(DATA_BLOCK, i));
      leaf.close();
    }

    // Check the number of data in both pages
    int count = 0;
    leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, insertRange, tx);
    while (leaf.next()) count++;
    leaf.close();

    Assert.assertEquals(numOfRecords, count);
  }

  @Test
  public void testSplit() {
    BlockId blk = new BlockId(INDEX_FILE_NAME, 3);
    BlockId newBlk = null;
    BTreeLeaf leaf;
    DirEntry dirEntry;
    SearchRange insertRange;

    // Insert a lot of records with different keys
    int numOfRecords = MAX_NUM_OF_RECORDS * 3 / 2;
    for (int i = 0; i < numOfRecords; i++) {
      insertRange = newSearchRange(i);
      leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, insertRange, tx);
      dirEntry = leaf.insert(new RecordId(DATA_BLOCK, i));
      if (dirEntry != null) newBlk = new BlockId(INDEX_FILE_NAME, dirEntry.blockNumber());
      leaf.close();
    }

    if (newBlk == null) Assert.fail("BTreeLeafTest: Bad split");

    // Check the number of data in both pages
    SearchRange searchRange = newSearchRange(0, numOfRecords - 1);
    int count = 0;
    leaf = new BTreeLeaf(DATA_BLOCK.fileName(), blk, KEY_TYPE, searchRange, tx);
    while (leaf.next()) count++;
    leaf.close();

    Assert.assertEquals(numOfRecords, count);
  }
}
