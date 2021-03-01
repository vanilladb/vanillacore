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
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class BTreePageTest {
  private static Logger logger = Logger.getLogger(BTreePageTest.class.getName());

  // the file starting with "_temp" will be deleted during initialization
  private static String FILE_PREFIX = "_test" + System.currentTimeMillis() + "_";
  private static final String FILE_NAME = FILE_PREFIX + "BtreePage.idx";
  private static final int NUM_FLAGS = 1;
  private static final Schema sch = new Schema();
  private static final String ID_FIELD_NAME = "ID";
  private static final int PRE_FORMATED_PAGE_COUNT = 4;

  private Transaction tx;

  static {
    sch.addField(ID_FIELD_NAME, Type.INTEGER);
    sch.addField("FIELD_DOUBLE", Type.DOUBLE);
    sch.addField("FIELD_BIGINT", Type.BIGINT);
    sch.addField("FIELD_VARCHAR(30)", Type.VARCHAR(30));
  }

  @BeforeClass
  public static void init() {
    ServerInit.init(BTreePageTest.class);
    RecoveryMgr.enableLogging(false);

    if (logger.isLoggable(Level.INFO)) logger.info("BEGIN BTREE PAGE TEST");

    Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);

    // Format the pages that will be used later
    for (int i = 0; i < PRE_FORMATED_PAGE_COUNT; i++)
      tx.bufferMgr().pinNew(FILE_NAME, new BTPageFormatter(sch, new long[] {0}));

    tx.commit();
  }

  @AfterClass
  public static void finish() {
    RecoveryMgr.enableLogging(true);

    if (logger.isLoggable(Level.INFO)) logger.info("FINISH BTREE PAGE TEST");
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
    BlockId blk = new BlockId(FILE_NAME, 0);
    BTreePage page = new BTreePage(blk, NUM_FLAGS, sch, tx);

    // Insert 20 records
    int numOfRecords = 20;
    for (int i = 0; i < numOfRecords; i++) {
      page.insert(0);
      page.setVal(0, ID_FIELD_NAME, new IntegerConstant(numOfRecords - i - 1));
    }

    // Check number of records
    Assert.assertEquals(numOfRecords, page.getNumRecords());

    // Check the order of records
    for (int i = 0; i < numOfRecords; i++) {
      Assert.assertEquals(new IntegerConstant(i), page.getVal(i, ID_FIELD_NAME));
    }
  }

  @Test
  public void testDelete() {
    BlockId blk = new BlockId(FILE_NAME, 1);
    BTreePage page = new BTreePage(blk, NUM_FLAGS, sch, tx);

    // Insert 20 records
    int numOfRecords = 20;
    for (int i = 0; i < numOfRecords; i++) {
      page.insert(0);
      page.setVal(0, ID_FIELD_NAME, new IntegerConstant(numOfRecords - i - 1));
    }

    // Check number of records
    Assert.assertEquals(numOfRecords, page.getNumRecords());

    // Delete 20th, 15th, 10th, 5th record
    page.delete(19);
    page.delete(14);
    page.delete(9);
    page.delete(4);

    // Check number of records
    Assert.assertEquals(numOfRecords - 4, page.getNumRecords());

    // Check the contents of the rest records
    for (int slotId = 0, id = 0; slotId < numOfRecords - 4; slotId++, id++) {
      if (id == 4 || id == 9 || id == 14 || id == 19) id++;

      Assert.assertEquals(new IntegerConstant(id), page.getVal(slotId, ID_FIELD_NAME));
    }
  }

  @Test
  public void testTransferRecords() {
    BlockId blk1 = new BlockId(FILE_NAME, 2);
    BlockId blk2 = new BlockId(FILE_NAME, 3);
    BTreePage page1 = new BTreePage(blk1, NUM_FLAGS, sch, tx);
    BTreePage page2 = new BTreePage(blk2, NUM_FLAGS, sch, tx);

    // Insert 20 records
    int numOfRecords = 20;
    for (int i = 0; i < numOfRecords; i++) {
      page1.insert(0);
      page1.setVal(0, ID_FIELD_NAME, new IntegerConstant(numOfRecords - i - 1));
    }

    // Check number of records
    Assert.assertEquals(numOfRecords, page1.getNumRecords());

    // Transfer 10 records to page2
    page1.transferRecords(numOfRecords / 2, page2, 0, numOfRecords / 2);

    // Check the records in both pages
    for (int i = 0; i < numOfRecords / 2; i++) {
      Assert.assertEquals(new IntegerConstant(i), page1.getVal(i, ID_FIELD_NAME));
      Assert.assertEquals(
          new IntegerConstant(numOfRecords / 2 + i), page2.getVal(i, ID_FIELD_NAME));
    }
  }

  @Test
  public void testSplits() {
    BlockId blk1 = new BlockId(FILE_NAME, 4);
    BTreePage page1 = new BTreePage(blk1, NUM_FLAGS, sch, tx);

    // Insert 20 records
    int numOfRecords = 20;
    for (int i = 0; i < numOfRecords; i++) {
      page1.insert(0);
      page1.setVal(0, ID_FIELD_NAME, new IntegerConstant(numOfRecords - i - 1));
    }

    // Check number of records
    Assert.assertEquals(numOfRecords, page1.getNumRecords());

    // Split the page
    long blkNum = page1.split(10, new long[] {1});
    BlockId blk2 = new BlockId(FILE_NAME, blkNum);
    BTreePage page2 = new BTreePage(blk2, NUM_FLAGS, sch, tx);

    // Check the flag
    Assert.assertEquals(1, page2.getFlag(0));

    // Check the records in both pages
    for (int i = 0; i < numOfRecords / 2; i++) {
      Assert.assertEquals(new IntegerConstant(i), page1.getVal(i, ID_FIELD_NAME));
      Assert.assertEquals(
          new IntegerConstant(numOfRecords / 2 + i), page2.getVal(i, ID_FIELD_NAME));
    }
  }
}
