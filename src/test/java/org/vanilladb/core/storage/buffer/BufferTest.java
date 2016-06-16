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
import static org.junit.Assert.fail;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.tx.Transaction;

public class BufferTest {
	private static Logger logger = Logger.getLogger(BufferTest.class.getName());
	
	private static String fileName = "_tempbuffercctest";
	
	@BeforeClass
	public static void init() {
		ServerInit.init(BufferTest.class);
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN BUFFER TEST");
	}
	
	@Test
	public void testBuffer() {
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		BufferMgr bm = tx.bufferMgr();
		
		BlockId blk = new BlockId(fileName, 3);
		Buffer buff = bm.pin(blk);
		
		// fill the buffer with some values, but don't bother to log them
		int pos = 0;
		while (true) {
			if (pos + Page.maxSize(INTEGER) >= Buffer.BUFFER_SIZE)
				break;
			int val = 1000 + pos;
			buff.setVal(pos, new IntegerConstant(val), 1, null);
			pos += Page.maxSize(INTEGER);
			String s = "value" + pos;
			int strlen = Page.maxSize(VARCHAR(s.length()));
			if (pos + strlen >= Page.BLOCK_SIZE)
				break;
			buff.setVal(pos, new VarcharConstant(s), 1, null);
			pos += strlen;
		}
		bm.unpin(buff);

		Buffer buff2 = bm.pin(blk);
		pos = 0;
		while (true) {
			if (pos + Page.maxSize(INTEGER) >= Buffer.BUFFER_SIZE)
				break;
			int val = 1000 + pos;
			assertEquals("*****BufferTest: bad getInt", (Integer) val,
					(Integer) buff2.getVal(pos, INTEGER).asJavaVal());
			pos += Page.maxSize(INTEGER);
			String s = "value" + pos;
			int strlen = Page.maxSize(VARCHAR(s.length()));
			if (pos + strlen >= Page.BLOCK_SIZE)
				break;
			assertEquals("*****BufferTest: bad getString", s, (String) buff2
					.getVal(pos, VARCHAR).asJavaVal());
			pos += strlen;
		}
		bm.unpin(buff2);
	}

	@Test
	public void testMultiplePinning() {
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		BufferMgr bm = tx.bufferMgr();
		
		// Pin block 0 (#available - 1)
		int avail1 = bm.available();
		BlockId blk1 = new BlockId(fileName, 0);
		Buffer buff1 = bm.pin(blk1);
		int avail2 = bm.available();
		assertEquals("*****BufferTest: bad available", avail1 - 1, avail2);
		
		// Pin block 1 (#available - 1)
		BlockId blk2 = new BlockId(fileName, 1);
		Buffer buff2 = bm.pin(blk2);
		int avail3 = bm.available();
		assertEquals("*****BufferTest: bad available", avail2 - 1, avail3);
		
		// Pin block 0 (#available no change)
		BlockId blk3 = new BlockId(fileName, 0);
		Buffer buff3 = bm.pin(blk3);
		int avail4 = bm.available();
		assertEquals("*****BufferTest: bad available", avail3, avail4);
		
		// Unpin block 0 (#available no change)
		bm.unpin(buff1);
		int avail5 = bm.available();
		assertEquals("*****BufferTest: bad available", avail4, avail5);
		
		// Unpin block 1 (#available + 1)
		bm.unpin(buff2);
		int avail6 = bm.available();
		assertEquals("*****BufferTest: bad available", avail5 + 1, avail6);
		
		// Unpin block 0 (#available + 1)
		bm.unpin(buff3);
		int avail7 = bm.available();
		assertEquals("*****BufferTest: bad available", avail6 + 1, avail7);
		
		// #available should be the same as the start time
		assertEquals("*****BufferTest: bad available", avail7, avail1);
	}

	/**
	 * Tests the buffer manager when a transaction requires buffers more than
	 * the buffer pool size.
	 */
	@Test
	public void testBufferAbortException() {
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		
		ArrayList<Buffer> pinnedBuff = new ArrayList<Buffer>();
		try {
			int i = 0;
			while (i <= BufferMgr.BUFFER_POOL_SIZE) {
				BlockId blk = new BlockId(fileName, i);
				pinnedBuff.add(tx.bufferMgr().pin(blk));
				i++;
			}
			fail("*****BufferTest: bad bufferAbortException");
		} catch (BufferAbortException e) {
			for (Buffer buf : pinnedBuff)
				tx.bufferMgr().unpin(buf);
		}
	}
}
