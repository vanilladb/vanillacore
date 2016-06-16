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
import org.vanilladb.core.util.BarrierStartRunner;

import junit.framework.Assert;

public class BufferConcurrencyTest {

	private static final int CLIENT_COUNT = 100;

	@BeforeClass
	public static void init() {
		ServerInit.init(BufferConcurrencyTest.class);
	}

	@Test
	public void testConcourrentPinning() {
		Buffer buffer = new Buffer();
		CyclicBarrier startBarrier = new CyclicBarrier(CLIENT_COUNT);
		CyclicBarrier endBarrier = new CyclicBarrier(CLIENT_COUNT + 1);

		// Create multiple threads
		for (int i = 0; i < CLIENT_COUNT; i++)
			new Pinner(startBarrier, endBarrier, buffer).start();

		// Wait for running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		// Check the results
		Assert.assertEquals("testBufferPinCount failed", buffer.isPinned(), false);
	}

	class Pinner extends BarrierStartRunner {

		Buffer buf;

		public Pinner(CyclicBarrier startBarrier, CyclicBarrier endBarrier, Buffer buf) {
			super(startBarrier, endBarrier);

			this.buf = buf;
		}

		@Override
		public void runTask() {
			for (int i = 0; i < 10000; i++) {
				buf.pin();
				buf.unpin();
			}
		}

	}
}
