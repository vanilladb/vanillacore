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
	
	private static final String TEST_FILE_NAME = "_tempbuffermgrtest";
	
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
	public void testRepinning() {
		Transaction initTx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		
		// leave only two buffers available in buffer pool
		int avail = initTx.bufferMgr().available();
		for (int i = 0; i < avail - 2; i++) {
			// leave blocks 0 to 3 for testing
			BlockId blk = new BlockId(TEST_FILE_NAME, i + 4);
			initTx.bufferMgr().pin(blk);
		}
		
		// start testing
		try {
			TxClientD thD = new TxClientD(0, 1000);
			thD.start();
			TxClientE thE = new TxClientE(500, 1500);
			thE.start();
			try {
				thD.join();
				thE.join();
			} catch (InterruptedException e) {
			}
			String expected = "Tx D: pin 1 start\n" + "Tx D: pin 1 end\n"
					+ "Tx E: pin 2 start\n" + "Tx E: pin 2 end\n"
					+ "Tx D: pin 3 start\n" + "Tx E: pin 4 start\n"
					+ "Tx E: pin 4 end\n" + "Tx D: pin 3 end\n";
			assertEquals("*****TxTest: bad tx history", expected, result);
		} finally {
			initTx.rollback();
		}
	}

	synchronized static void appendToResult(String s) {
		result += s + "\n";
	}
	
	abstract class TxClient extends Thread {
		protected int[] pauses;
		protected boolean deadlockAborted;

		TxClient(int... pauses) {
			this.pauses = pauses;
		}

		boolean isDeadlockAborted() {
			return deadlockAborted;
		}
	}

	class TxClientD extends TxClient {
		TxClientD(int... pauses) {
			super(pauses);
		}

		@Override
		public void run() {
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			try {
				if (pauses[0] > 0)
					Thread.sleep(pauses[0]);

				appendToResult("Tx D: pin 1 start");
				BlockId blk1 = new BlockId(TEST_FILE_NAME, 0);
				tx.bufferMgr().pin(blk1);
				appendToResult("Tx D: pin 1 end");

				if (pauses[1] > 0)
					Thread.sleep(pauses[1]);

				appendToResult("Tx D: pin 3 start");
				BlockId blk3 = new BlockId(TEST_FILE_NAME, 2);
				tx.bufferMgr().pin(blk3);
				appendToResult("Tx D: pin 3 end");
			} catch (InterruptedException e) {
			} finally {
				tx.rollback();
			}
		}
	}

	class TxClientE extends TxClient {
		TxClientE(int... pauses) {
			super(pauses);
		}

		@Override
		public void run() {
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			try {
				if (pauses[0] > 0)
					Thread.sleep(pauses[0]);

				appendToResult("Tx E: pin 2 start");
				BlockId blk2 = new BlockId(TEST_FILE_NAME, 1);
				tx.bufferMgr().pin(blk2);
				appendToResult("Tx E: pin 2 end");

				if (pauses[1] > 0)
					Thread.sleep(pauses[1]);

				appendToResult("Tx E: pin 4 start");
				BlockId blk4 = new BlockId(TEST_FILE_NAME, 3);
				tx.bufferMgr().pin(blk4);
				appendToResult("Tx E: pin 4 end");
			} catch (InterruptedException e) {
			} finally {
				tx.rollback();
			}
		}
	}
}


