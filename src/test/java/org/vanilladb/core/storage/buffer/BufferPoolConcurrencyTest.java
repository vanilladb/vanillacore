/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
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
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.util.BarrierStartRunner;

public class BufferPoolConcurrencyTest {
	private static Logger logger = Logger.getLogger(BufferPoolConcurrencyTest.class.getName());

	private static final int CLIENT_COUNT_PER_BUFFER = 100;
	private static final int BUFFER_COUNT = 10;
	private static final int PIN_PER_CLIENT = 1000;

	private static final String TEST_FILE_NAME = "_tempbufferpooltest";

	@BeforeClass
	public static void init() {
		String dbName = ServerInit.resetDb(BufferPoolConcurrencyTest.class);
		VanillaDb.initFileMgr(dbName);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN BUFFER POOL CONCURRENCY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH BUFFER POOL CONCURRENCY TEST");
	}

	@Test
	public void testSwapping() {
		int clientCount = BUFFER_COUNT * 2;
		BufferPoolMgr bufferPool = new BufferPoolMgr(BUFFER_COUNT);
		CyclicBarrier startBarrier = new CyclicBarrier(clientCount);
		CyclicBarrier endBarrier = new CyclicBarrier(clientCount + 1);
		Pinner[] pinners = new Pinner[clientCount];

		// Create threads
		for (int pid = 0; pid < clientCount; pid++) {
			pinners[pid] = new Pinner(startBarrier, endBarrier, bufferPool,
					new BlockId(TEST_FILE_NAME, pid));
			pinners[pid].start();
		}

		// Wait for all the clients
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		// Check if there is any exception
		for (int pid = 0; pid < clientCount; pid++) {
			if (pinners[pid].hasException()) {
				pinners[pid].printExceptionStackTrace();
				Assert.fail(pinners[pid].getExceptionDescription());
			}
		}
	}

	@Test
	public void testConcourrentPinning() {
		int clientCount = BUFFER_COUNT * CLIENT_COUNT_PER_BUFFER;
		BufferPoolMgr bufferPool = new BufferPoolMgr(BUFFER_COUNT);
		CyclicBarrier startBarrier = new CyclicBarrier(clientCount);
		CyclicBarrier endBarrier = new CyclicBarrier(clientCount + 1);
		RetainBufferPinner[] pinners = new RetainBufferPinner[clientCount];

		// Create multiple threads
		for (int blkNum = 0; blkNum < BUFFER_COUNT; blkNum++)
			for (int i = 0; i < CLIENT_COUNT_PER_BUFFER; i++) {
				pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i] = new RetainBufferPinner(startBarrier, endBarrier, bufferPool,
						new BlockId(TEST_FILE_NAME, blkNum));
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
				if (pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i].hasException()) {
					pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i].printExceptionStackTrace();
					Assert.fail(pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i].getExceptionDescription());
				}
				
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

		public Pinner(CyclicBarrier startBarrier, CyclicBarrier endBarrier, BufferPoolMgr bufferPool, BlockId blk) {
			super(startBarrier, endBarrier);

			this.bufferPool = bufferPool;
			this.blk = blk;
		}

		@Override
		public void runTask() {
			try {
				for (int i = 0; i < PIN_PER_CLIENT; i++) {
					pin();
					
					// Check if the buffer contains the block the thread wants
					// This may fail when the buffers are not protected while swapping
					if (!buf.block().equals(blk)) {
						throw new RuntimeException("swapping fails for blk: " + blk);
					}
					
					unpin();
				}
			} finally {
				synchronized (bufferPool) {
					bufferPool.notifyAll();
				}
			}
		}
		
		private void pin() {
			buf = bufferPool.pin(blk);
			
			// Handles the case of not enough buffer
			while (buf == null) {
				synchronized (bufferPool) {
					try {
						bufferPool.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				buf = bufferPool.pin(blk);
			}
		}
		
		private void unpin() {
			bufferPool.unpin(buf);
			buf = null;
			synchronized (bufferPool) {
				bufferPool.notifyAll();
			}
		}
	}
	
	class RetainBufferPinner extends Pinner {
		
		public RetainBufferPinner(CyclicBarrier startBarrier, CyclicBarrier endBarrier,
				BufferPoolMgr bufferPool, BlockId blk) {
			super(startBarrier, endBarrier, bufferPool, blk);
		}

		@Override
		public void runTask() {
			super.runTask();
			
			// Pin one more time to retain the buffer
			buf = bufferPool.pin(blk);
		}
		
	}
}
