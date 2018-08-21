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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.log.LogSeqNum;

import junit.framework.Assert;

public class LastLSNTest {
	private static Logger logger = Logger.getLogger(LastLSNTest.class.getName());
	
	private static final String TEST_FILE_NAME = "last_lsn_test";
	
	private static final Constant[] values = new Constant[] {
		new IntegerConstant(0),
		new VarcharConstant("YS"),
		new BigIntConstant(10l),
		new DoubleConstant(0.0),
		new DoubleConstant(1.0),
		new VarcharConstant("netdb"),
		new VarcharConstant("kerker"),
		new IntegerConstant(101)
	};
	
	@BeforeClass
	public static void init() {
		ServerInit.init(LastLSNTest.class);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN LAST LSN TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH LAST LSN TEST");
	}
	
	@Test
	public void testFormattedPageCorrectness() {
		Buffer buf = new Buffer();
		buf.assignToNew(TEST_FILE_NAME + "1", new TestPageFormatter());
		buf.flush();
		
		// Reading values from the buffer
		int offset = 0;
		
		for (Constant val : values) {
			Assert.assertEquals(val, buf.getVal(offset, val.getType()));
			offset += Page.maxSize(val.getType());
		}
		
		// Reading values directly from the page
		Page page = buf.getUnderlyingPage();
		offset = LogSeqNum.SIZE;
		
		for (Constant val : values) {
			Assert.assertEquals(val, page.getVal(offset, val.getType()));
			offset += Page.maxSize(val.getType());
		}
	}
	
	@Test
	public void testLastLSN() {
		Buffer buf = new Buffer();
		BlockId blk = new BlockId(TEST_FILE_NAME + "2", 0);
		long txNum = 1;
		
		// Check the last LSN in memory
		buf.assignToBlock(blk);
		for (int i = 0; i < values.length; i++) {
			LogSeqNum lsn = new LogSeqNum(0, i);
			buf.setVal(0, values[i], txNum, lsn);
		}
		Assert.assertEquals(new LogSeqNum(0, values.length - 1), buf.lastLsn());
		
		// Check the last LSN loaded from the file by another buffer
		buf.flush();
		Buffer buf2 = new Buffer();
		buf2.assignToBlock(blk);
		Assert.assertEquals(new LogSeqNum(0, values.length - 1), buf.lastLsn());
	}
	
	/**
	 * Initialize the page as follows:
	 * 
	 * [0, 'YS', 10l, 0.0, 1.0, 'netdb', 'kerker', 101]
	 * 
	 * @author SLMT
	 *
	 */
	private static class TestPageFormatter extends PageFormatter {
		
		@Override
		public void format(Buffer buf) {
			int offset = 0;
			
			for (Constant val : values) {
				setVal(buf, offset, val);
				offset += Page.maxSize(val.getType());
			}
			
		}
		
	}
}
