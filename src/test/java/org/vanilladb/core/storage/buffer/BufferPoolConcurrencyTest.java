/* Copyright 2016-2021 vanilladb.org contributors*/
package org.vanilladb.core.storage.buffer;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.util.BarrierStartRunner;

public class BufferPoolConcurrencyTest {
  private static Logger logger = Logger.getLogger(BufferPoolConcurrencyTest.class.getName());

  private static final int CLIENT_COUNT_PER_BUFFER = 100;
  private static final int BUFFER_COUNT = 10;
  private static final int TOTAL_CLIENT_COUNT = BUFFER_COUNT * CLIENT_COUNT_PER_BUFFER;

  private static final String TEST_FILE_NAME = "_tempbufferpooltest";

  @BeforeClass
  public static void init() {
    ServerInit.init(BufferPoolConcurrencyTest.class);

    if (logger.isLoggable(Level.INFO)) logger.info("BEGIN BUFFER POOL CONCURRENCY TEST");
  }

  @AfterClass
  public static void finish() {
    if (logger.isLoggable(Level.INFO)) logger.info("FINISH BUFFER POOL CONCURRENCY TEST");
  }

  @Test
  public void testConcourrentPinning() {
    BufferPoolMgr bufferPool = new BufferPoolMgr(BUFFER_COUNT);
    CyclicBarrier startBarrier = new CyclicBarrier(TOTAL_CLIENT_COUNT);
    CyclicBarrier endBarrier = new CyclicBarrier(TOTAL_CLIENT_COUNT + 1);
    Pinner[] pinners = new Pinner[TOTAL_CLIENT_COUNT];

    // Create multiple threads
    for (int blkNum = 0; blkNum < BUFFER_COUNT; blkNum++)
      for (int i = 0; i < CLIENT_COUNT_PER_BUFFER; i++) {
        pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i] =
            new Pinner(startBarrier, endBarrier, bufferPool, new BlockId(TEST_FILE_NAME, blkNum));
        pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i].start();
      }

    // Wait for running
    try {
      endBarrier.await();
    } catch (InterruptedException | BrokenBarrierException e) {
      e.printStackTrace();
    }

    // Check the results
    for (int blkNum = 0; blkNum < BUFFER_COUNT; blkNum++) {
      Buffer buffer = pinners[blkNum * CLIENT_COUNT_PER_BUFFER].buf;

      for (int i = 0; i < CLIENT_COUNT_PER_BUFFER; i++) {

        // Check if there is any exception
        if (pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i].getException() != null)
          Assert.fail(
              "Exception happens: "
                  + pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i].getException().getMessage());

        // The threads using the same block id should get the
        // same buffer
        if (buffer != pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i].buf)
          Assert.fail("Thread no." + i + " for block no." + blkNum + " get a wrong buffer");
      }
    }
  }

  class Pinner extends BarrierStartRunner {

    BufferPoolMgr bufferPool;
    BlockId blk;
    Buffer buf;

    public Pinner(
        CyclicBarrier startBarrier,
        CyclicBarrier endBarrier,
        BufferPoolMgr bufferPool,
        BlockId blk) {
      super(startBarrier, endBarrier);

      this.bufferPool = bufferPool;
      this.blk = blk;
    }

    @Override
    public void runTask() {
      for (int i = 0; i < 100; i++) {
        buf = bufferPool.pin(blk);
        bufferPool.unpin(buf);
      }
      buf = bufferPool.pin(blk);
    }
  }
}
