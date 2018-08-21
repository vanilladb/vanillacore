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
package org.vanilladb.core.storage.file;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.util.BarrierStartRunner;

import junit.framework.Assert;

public class PageConcurrencyTest {
	private static Logger logger = Logger.getLogger(PageConcurrencyTest.class.getName());

	private static final int CLIENT_COUNT = 10;
	private static final int ITERATION_COUNT = 1000;
	
	@BeforeClass
	public static void init() {
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN PAGE CONCURRENCY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH PAGE CONCURRENCY TEST");
	}

	@Test
	public void testConcurrentSet() {
		Page page = new Page();
		CyclicBarrier startBarrier = new CyclicBarrier(CLIENT_COUNT);
		CyclicBarrier endBarrier = new CyclicBarrier(CLIENT_COUNT + 1);

		// Create multiple threads
		Constant[] values = new Constant[CLIENT_COUNT];
		for (int i = 0; i < CLIENT_COUNT; i++) {
			values[i] = new BigIntConstant(i);
			new SetValueClient(page, i * 8, values[i], startBarrier, endBarrier).start();
		}

		// Wait for running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		// Check the results
		for (int i = 0; i < CLIENT_COUNT; i++) {
			Constant result = page.getVal(i * 8, Type.BIGINT);
			Assert.assertEquals(result, values[i]);
		}
	}

	@Test
	public void testConcurrentGetAndSet() {
		Page page = new Page();
		CyclicBarrier startBarrier = new CyclicBarrier(CLIENT_COUNT);
		CyclicBarrier endBarrier = new CyclicBarrier(CLIENT_COUNT + 1);

		// Prepare testing values
		Constant[] values = new Constant[CLIENT_COUNT];
		for (int i = 0; i < CLIENT_COUNT; i++) {
			values[i] = new BigIntConstant(i);
			page.setVal(i * 8, values[i]);
		}

		// Create multiple threads
		GetValueClient[] gClients = new GetValueClient[CLIENT_COUNT];
		for (int i = 0; i < CLIENT_COUNT; i++) {
			gClients[i] = new GetValueClient(page, i * 8, values[i], startBarrier, endBarrier);

			new SetValueClient(page, i * 8, values[i], startBarrier, endBarrier).start();
			gClients[i].start();
		}

		// Wait for running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		// Check if there is any error
		for (int i = 0; i < CLIENT_COUNT; i++) {
			if (gClients[i].error != null)
				Assert.fail(gClients[i].error);
		}
	}

	class SetValueClient extends BarrierStartRunner {
		Page page;
		int offset;
		Constant value;

		public SetValueClient(Page page, int offset, Constant value, CyclicBarrier startBarrier,
				CyclicBarrier endBarrier) {
			super(startBarrier, endBarrier);
			this.page = page;
			this.offset = offset;
			this.value = value;
		}

		@Override
		public void runTask() {
			for (int i = 0; i < ITERATION_COUNT; i++)
				page.setVal(offset, value);
		}

	}

	class GetValueClient extends BarrierStartRunner {
		Page page;
		int offset;
		Constant value;
		String error;

		public GetValueClient(Page page, int offset, Constant value, CyclicBarrier startBarrier,
				CyclicBarrier endBarrier) {
			super(startBarrier, endBarrier);
			this.page = page;
			this.offset = offset;
			this.value = value;
		}

		@Override
		public void runTask() {
			Constant result;

			for (int i = 0; i < ITERATION_COUNT; i++) {
				result = page.getVal(offset, Type.BIGINT);
				if (!result.equals(value)) {
					error = "expected:<" + value + ">, but was:<" + result + "> at offset: " + offset;
					break;
				}
			}
		}

	}
}
