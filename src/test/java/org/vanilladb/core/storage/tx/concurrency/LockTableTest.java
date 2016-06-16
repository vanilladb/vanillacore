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
package org.vanilladb.core.storage.tx.concurrency;

import static org.junit.Assert.fail;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;

public class LockTableTest {
	private static Logger logger = Logger.getLogger(LockTableTest.class
			.getName());

	private static String fileName = "_testlocktable.0";
	private static int max = 100;
	private static BlockId[] blocks;
	private static RecordId[] records;
	private static long txNum1 = 1;
	private static long txNum2 = 2;

	private static LockTable lockTbl;

	@BeforeClass
	public static void init() {
		ServerInit.init(LockTableTest.class);
		
		// Prepare testing data
		blocks = new BlockId[max];
		records = new RecordId[max];
		for (int i = 0; i < max; i++) {
			blocks[i] = new BlockId(fileName, i);
			records[i] = new RecordId(blocks[i], 5);
		}
		
		// Initialize LockTable
		lockTbl = new LockTable();
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN LOCK TABLE_ONLY TEST");
	}

	@Before
	public void setup() {
		lockTbl.releaseAll(txNum1, false);
		lockTbl.releaseAll(txNum2, false);
	}

	@Test
	public void testSLocks() {
		try {
			for (int i = 0; i < max; i++) {
				lockTbl.sLock(blocks[i], txNum1);
				lockTbl.sLock(blocks[i], txNum1);
				lockTbl.sLock(blocks[i], txNum2);
			}

			lockTbl.releaseAll(txNum1, false);
			lockTbl.releaseAll(txNum2, false);
		} catch (LockAbortException e) {
			fail("*****LockTableTest: bad slocks");
		}
	}

	@Test
	public void testXLocks() {
		try {
			/*
			 * subsequent lock requests are ignored if a transaction has an
			 * xlock
			 */
			lockTbl.xLock(blocks[0], txNum1);
			lockTbl.xLock(blocks[0], txNum1); // ignored
			lockTbl.sLock(blocks[0], txNum1);
			lockTbl.sLock(blocks[0], txNum1); // ignored

			lockTbl.sLock(blocks[1], txNum1);
			lockTbl.sLock(blocks[1], txNum1); // ignored
			lockTbl.xLock(blocks[1], txNum1); // upgraded
			lockTbl.xLock(blocks[1], txNum1); // ignored
		} catch (LockAbortException e) {
			fail("*****LockTableTest: bad xlocks");
		}
		
		try {
			lockTbl.sLock(blocks[0], txNum2);
			lockTbl.sLock(blocks[1], txNum2);
			fail("*****LockTableTest: slock allowed after xlock");
		} catch (LockAbortException e) {
		}
		
		try {
			lockTbl.xLock(blocks[0], txNum2);
			lockTbl.xLock(blocks[1], txNum2);
			fail("*****LockTableTest: xlock allowed after xlock");
		} catch (LockAbortException e) {
		}
		
		try {
			lockTbl.releaseAll(txNum1, false);
			lockTbl.xLock(blocks[0], txNum2);
			lockTbl.xLock(blocks[1], txNum2);
			lockTbl.releaseAll(txNum2, false);
		} catch (LockAbortException e) {
			fail("*****LockTableTest: bad release");
		}
	}
	
	// TODO: We need a deadlock avoidance test case

	@Test
	public void testMultiGranularityLocking() {
		try {
			lockTbl.isLock(fileName, txNum1);
			for (int i = 0; i < max; i++) {
				lockTbl.isLock(blocks[i], txNum1);
				lockTbl.sLock(records[i], txNum1);
			}

			try {
				lockTbl.isLock(fileName, txNum2); // not the only isLocker
				lockTbl.xLock(fileName, txNum2);
				fail("*****LockTableTest: xlock allowed after islock");
			} catch (LockAbortException e) {
			}

			try {
				lockTbl.xLock(blocks[0], txNum2);
				fail("*****LockTableTest: xlock allowed after islock");
			} catch (LockAbortException e) {
			}

			try {
				lockTbl.ixLock(fileName, txNum2);
			} catch (LockAbortException e) {
				fail("*****LockTableTest: ixlock disallowed after islock");
			}

			try {
				lockTbl.sixLock(blocks[3], txNum2);
			} catch (LockAbortException e) {
				fail("*****LockTableTest: sixlock disallowed after islock");
			}

			lockTbl.releaseAll(txNum1, false);
			lockTbl.releaseAll(txNum2, false);

			for (int i = 0; i < max; i++) {
				lockTbl.isLock(fileName, txNum1); // test is the only islocker
				lockTbl.isLock(blocks[i], txNum1);
				lockTbl.sLock(records[i], txNum1);
			}

			try {
				lockTbl.xLock(fileName, txNum1);
			} catch (LockAbortException e) {
				fail("*****LockTableTest: xlock disallowed when it is the only islocker");
			}

			try {
				lockTbl.ixLock(fileName, txNum1);
			} catch (LockAbortException e) {
				fail("*****LockTableTest: ixlock disallowed when it is the only islocker/xlocker");
			}

			try {
				lockTbl.sixLock(blocks[7], txNum1);
			} catch (LockAbortException e) {
				fail("*****LockTableTest: sixlock disallowed when it is the only xlocker/ixlocker/islocker on the parent node");
			}
			lockTbl.releaseAll(txNum1, false);

			try {
				lockTbl.sLock(fileName, txNum1);
				lockTbl.sLock(fileName, txNum2);
				lockTbl.ixLock(fileName, txNum2);
				fail("*****LockTableTest: ixlock allowed after slock");
			} catch (LockAbortException e) {

			}
			lockTbl.releaseAll(txNum1, false);
			lockTbl.releaseAll(txNum2, false);

			try {
				lockTbl.sLock(fileName, txNum1);
				lockTbl.sLock(fileName, txNum2);
				lockTbl.sixLock(fileName, txNum2);
				fail("*****LockTableTest: sixlock allowed after slock");
			} catch (LockAbortException e) {

			}
			lockTbl.releaseAll(txNum1, false);
			lockTbl.releaseAll(txNum2, false);

			try {
				lockTbl.ixLock(fileName, txNum1);
				lockTbl.ixLock(fileName, txNum2); // not the only ixLocker
				lockTbl.xLock(fileName, txNum2);
				fail("*****LockTableTest: xlock allowed after ixlock");
			} catch (LockAbortException e) {
			}

			lockTbl.releaseAll(txNum1, false);
			lockTbl.releaseAll(txNum2, false);

			try {
				lockTbl.ixLock(fileName, txNum1);
				lockTbl.ixLock(fileName, txNum2); // not the only ixLocker
				lockTbl.sLock(fileName, txNum2);
				fail("*****LockTableTest: slock allowed after ixlock");
			} catch (LockAbortException e) {
			}

			lockTbl.releaseAll(txNum1, false);
			lockTbl.releaseAll(txNum2, false);
		} catch (LockAbortException e) {
			fail("*****LockTableTest: bad slocks");
		}
	}
}
