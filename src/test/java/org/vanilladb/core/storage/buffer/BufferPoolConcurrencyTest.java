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
package org.vanilladb.core.storage.buffer;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.util.BarrierStartRunner;

import junit.framework.Assert;

public class BufferPoolConcurrencyTest {

	private static final int CLIENT_COUNT_PER_BUFFER = 100;
	private static final int BUFFER_COUNT = 100;
	private static final int TOTAL_CLIENT_COUNT = BUFFER_COUNT * CLIENT_COUNT_PER_BUFFER;

	private static final String TEST_FILE_NAME = "_tempbufferpooltest";

	@BeforeClass
	public static void init() {
		ServerInit.init(BufferPoolConcurrencyTest.class);
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
				pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i] = new Pinner(startBarrier, endBarrier, bufferPool,
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
				if (pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i].getException() != null)
					Assert.fail("Exception happens: " + pinners[blkNum * CLIENT_COUNT_PER_BUFFER + i]
							.getException().getMessage());
				
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
			for (int i = 0; i < 100; i++) {
				buf = bufferPool.pin(blk);
				bufferPool.unpin(buf);
			}
			buf = bufferPool.pin(blk);
		}

	}
}
