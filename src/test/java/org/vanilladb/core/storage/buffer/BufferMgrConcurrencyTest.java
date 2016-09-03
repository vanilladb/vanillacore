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

import static org.junit.Assert.assertEquals;

import java.sql.Connection;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * All blocks used by this class should be numbered from 0 to
 * {@link VanillaDb#BUFFER_SIZE}.
 */
public class BufferMgrConcurrencyTest {
	
	private static final String TEST_FILE1_NAME = "_tempbufferconmgrtest1";
	private static final String TEST_FILE2_NAME = "_tempbufferconmgrtest2";
	
	private static String result = "";
	
	@BeforeClass
	public static void init() {
		ServerInit.init(BufferMgrConcurrencyTest.class);
	}
	
	@Before
	public void before() {
		result = "";
	}

	@Test
	public void testConcurrentRepinning() {
		Transaction initTx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		
		// leave 11 buffers available in buffer pool
		int avail = initTx.bufferMgr().available();
		for (int i = 0; i < avail - 11; i++) {
			BlockId blk = new BlockId(TEST_FILE1_NAME, i);
			initTx.bufferMgr().pin(blk);
		}
		
		// start testing
		try {
			TxClientA a = new TxClientA(0, 1000);
			a.start();
			TxClientB b = new TxClientB(500, 1500);
			b.start();
			try {
				a.join();
				b.join();
			} catch (InterruptedException e) {
			}
			
			// Generate the expected result
			StringBuilder expected = new StringBuilder();
			for (int blkNum = 0; blkNum < 10; blkNum++) {
				expected.append("Tx A: pin " + blkNum + " start\n");
				expected.append("Tx A: pin " + blkNum + " end\n");
			}
			expected.append("Tx B: pin 10 start\n");
			expected.append("Tx B: pin 10 end\n");
			expected.append("Tx A: pin 11 start\n");
			expected.append("Tx B: pin 12 start\n");
			expected.append("Tx B: pin 12 end\n");
			expected.append("Tx A: pin 11 end\n");
			
			assertEquals("*****TxTest: bad tx history", expected.toString(), result);
		} finally {
			initTx.rollback();
		}
	}

	synchronized static void appendToResult(String s) {
		result += s + "\n";
	}
	
	abstract class TxClient extends Thread {
		// TODO: It may be better to use barriers here, instead of using time slicing
		protected int[] pauses;
		protected boolean deadlockAborted;
		
		private String threadName;

		TxClient(String name, int... pauses) {
			this.threadName = name;
			this.pauses = pauses;
		}

		boolean isDeadlockAborted() {
			return deadlockAborted;
		}
		
		void pin(Transaction tx, int blkNum) {
			appendToResult(threadName + ": pin " + blkNum + " start");
			
			BlockId blk = new BlockId(TEST_FILE2_NAME, blkNum);
			tx.bufferMgr().pin(blk);
			
			appendToResult(threadName + ": pin " + blkNum + " end");
		}
	}

	class TxClientA extends TxClient {
		TxClientA(int... pauses) {
			super("Tx A", pauses);
		}

		@Override
		public void run() {
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			try {
				if (pauses[0] > 0)
					Thread.sleep(pauses[0]);
				
				// pin 0 ~ 9
				for (int blkNum = 0; blkNum < 10; blkNum++)
					pin(tx, blkNum);

				if (pauses[1] > 0)
					Thread.sleep(pauses[1]);
				
				// pin 11
				pin(tx, 11);
			} catch (InterruptedException e) {
			} finally {
				tx.rollback();
			}
		}
	}

	class TxClientB extends TxClient {
		TxClientB(int... pauses) {
			super("Tx B", pauses);
		}

		@Override
		public void run() {
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			try {
				if (pauses[0] > 0)
					Thread.sleep(pauses[0]);
				
				// pin 10
				pin(tx, 10);

				if (pauses[1] > 0)
					Thread.sleep(pauses[1]);
				
				// pin 12
				pin(tx, 12);
			} catch (InterruptedException e) {
			} finally {
				tx.rollback();
			}
		}
	}
}


