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
package org.vanilladb.core.storage.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;

public class FileTest {
	private static Logger logger = Logger.getLogger(FileTest.class.getName());

	// testing constants
	private static final IntegerConstant TEST_INT_123 = new IntegerConstant(123);
	private static final IntegerConstant TEST_INT_456 = new IntegerConstant(456);
	private static final IntegerConstant TEST_INT_789 = new IntegerConstant(789);
	private static final VarcharConstant TEST_VARCHAR = new VarcharConstant(
			"abcdefghijklmnopqrstuvwxyz0123456789");
	private static final int TEST_VARCHAR_SIZE = Page.size(TEST_VARCHAR);
	private static final int INT_SIZE = Page.maxSize(INTEGER);

	private static FileMgr fm;
	private static Page p1;
	private static Page p2;
	private static Page p3;

	@BeforeClass
	public static void init() {
		ServerInit.init(FileTest.class);

		fm = VanillaDb.fileMgr();
		p1 = new Page();
		p2 = new Page();
		p3 = new Page();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN FILE TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH FILE TEST");
	}

	@After
	public void setup() {
		// reset pages, fill them with 0s
		for (int pos = 0; pos + INT_SIZE < BLOCK_SIZE; pos += INT_SIZE) {
			p1.setVal(pos, new IntegerConstant(0));
			p2.setVal(pos, new IntegerConstant(0));
			p3.setVal(pos, new IntegerConstant(0));
		}
	}

	@Test
	public void testReadWriteAppend() {
		String filename = FileMgr.TMP_FILE_NAME_PREFIX + "_test_rwa";

		// write 123 and 456 at block 0
		BlockId blk = new BlockId(filename, 0);
		p1.setVal(0, TEST_INT_123);
		p1.setVal(INT_SIZE, TEST_INT_456);
		p1.write(blk);

		// read block 0 with another page and assert
		p2.read(blk);
		assertTrue("*****FileTest: bad getInt",
				p2.getVal(0, INTEGER).equals(TEST_INT_123)
						&& p2.getVal(INT_SIZE, INTEGER).equals(TEST_INT_456));

		// append the content of page 1 and test the block number of the new block
		long lastblock = fm.size(filename) - 1;
		BlockId blk2 = p1.append(filename);
		assertEquals("*****FileTest: bad append", lastblock + 1, blk2.number());

		// read the content of appended block and assert
		p2.read(blk2);
		assertTrue("*****FileTest: bad read",
				p2.getVal(0, INTEGER).equals(TEST_INT_123)
						&& p2.getVal(INT_SIZE, INTEGER).equals(TEST_INT_456));
	}

	@Test
	/**
	 * Test if FileMgr could automatically extend files to the writing position
	 */
	public void testFileList() {
		String filename = FileMgr.TMP_FILE_NAME_PREFIX + "_test_list";
		BlockId blk = new BlockId(filename, 14);
		p1.write(blk);
		assertEquals("*****FileTest: bad file list", 15, fm.size(filename));
	}

	@Test
	public void testSetAndGet() {
		// test normal get/set
		p1.setVal(0, TEST_INT_123);
		p1.setVal(20, TEST_VARCHAR);
		assertTrue(
				"*****FileTest: bad page get/set",
				p1.getVal(0, INTEGER).equals(TEST_INT_123)
						&& p1.getVal(20, VARCHAR).equals(TEST_VARCHAR));

		// test overlapping set int
		p1.setVal(2, TEST_INT_456);
		assertTrue(
				"*****FileTest: bad overlapping getInt",
				(!p1.getVal(0, INTEGER).equals(TEST_INT_123))
						&& p1.getVal(2, INTEGER).equals(TEST_INT_456));

		// test overlapping set varchar
		p1.setVal(26, TEST_VARCHAR);
		assertTrue(
				"*****FileTest: bad overlapping getString",
				(!p1.getVal(20, VARCHAR).equals(TEST_VARCHAR))
						&& p1.getVal(26, VARCHAR).equals(TEST_VARCHAR));

		// test contiguous set/get int
		p2.setVal(0, TEST_INT_123);
		p2.setVal(INT_SIZE, TEST_INT_456);
		p2.setVal(2 * INT_SIZE, TEST_INT_789);
		assertTrue(
				"*****FileTest: bad contiguous getInt",
				p2.getVal(0, INTEGER).equals(TEST_INT_123)
						&& p2.getVal(INT_SIZE, INTEGER).equals(TEST_INT_456)
						&& p2.getVal(INT_SIZE * 2, INTEGER)
								.equals(TEST_INT_789));
		
		// test contiguous set/get varchar
		p3.setVal(0, TEST_VARCHAR);
		p3.setVal(TEST_VARCHAR_SIZE, TEST_VARCHAR);
		p3.setVal(2 * TEST_VARCHAR_SIZE, TEST_VARCHAR);
		assertTrue(
				"*****FileTest: bad contiguous getString",
				p3.getVal(0, VARCHAR).equals(TEST_VARCHAR)
						&& p3.getVal(TEST_VARCHAR_SIZE, VARCHAR).equals(
								TEST_VARCHAR)
						&& p3.getVal(2 * TEST_VARCHAR_SIZE, VARCHAR).equals(
								TEST_VARCHAR));
	}

	@Test
	public void testBoundaries() {
		try {
			p1.setVal(-2, TEST_INT_123);
			fail("*****FileTest: allowed int negative offset");
		} catch (Exception e) {
		}
		try {
			p1.setVal(-2, TEST_VARCHAR);
			fail("*****FileTest: allowed String negative offset");
		} catch (Exception e) {
		}
		try {
			p1.setVal(BLOCK_SIZE - (INT_SIZE / 2), TEST_INT_123);
			fail("*****FileTest: allowed int large offset");
		} catch (Exception e) {
		}
		try {
			p1.setVal(BLOCK_SIZE - (TEST_VARCHAR_SIZE / 2), TEST_VARCHAR);
			fail("*****FileTest: allowed String large offset");
		} catch (Exception e) {
		}
	}

	@Test
	public void testBlockId() {
		BlockId b1 = new BlockId("abc", 0);
		BlockId b2 = new BlockId("def", 0);
		BlockId b3 = new BlockId(new String("ab" + "c"), 0);
		BlockId b4 = new BlockId("abc", 3);

		assertTrue("*****FileTest: bad block equals",
				!b1.equals(b2) && b1.equals(b3) && !b1.equals(b4));

		Map<BlockId, String> m = new HashMap<BlockId, String>();
		m.put(b1, "block 1");
		m.put(b2, "block 2");
		assertTrue(
				"*****FileTest: bad block hashcode",
				m.get(b1).equals("block 1") && m.get(b3).equals("block 1")
						&& m.get(b4) == null);

		BlockId b = new BlockId(b1.fileName(), b1.number());
		assertTrue("*****FileTest: bad block extraction", b.equals(b1));
	}
}
