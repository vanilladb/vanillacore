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

import java.nio.BufferOverflowException;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.io.IoAllocator;
import org.vanilladb.core.storage.file.io.IoBuffer;
import org.vanilladb.core.util.ByteHelper;
import org.vanilladb.core.util.CoreProperties;

/**
 * The contents of a disk block in memory. A page is treated as an array of
 * BLOCK_SIZE bytes. There are methods to get/set values into this array, and to
 * read/write the contents of this array to a disk block. For an example of how
 * to use Page and {@link BlockId} objects, consider the following code
 * fragment. The first portion increments the integer at offset 792 of block 6
 * of file junk. The second portion stores the string "hello" at offset 20 of a
 * page, and then appends it to a new block of the file. It then reads that
 * block into another page and extracts the value "hello" into variable s.
 * 
 * <pre>
 * Page p1 = new Page();
 * BlockId blk = new BlockId(&quot;junk&quot;, 6);
 * p1.read(blk);
 * Constant c = p1.getVal(792, new Type.INTEGER);
 * int n = (Integer) c.asJavaVal();
 * Constant v1 = new IntegerConstant(n);
 * p1.setVal(792, v1);
 * p1.write(blk);
 * 
 * Page p2 = new Page();
 * Constant v2 = new VarcharConstant(&quot;hello&quot;);
 * p2.setVal(20, v2);
 * blk = p2.append(&quot;junk&quot;);
 * Page p3 = new Page();
 * p3.read(blk);
 * String s = (String) p3.getVal(20).asJavaVal();
 * </pre>
 */

public class Page {

	/**
	 * The number of bytes in a block. A reasonable value would be 4K.
	 */
	public static final int BLOCK_SIZE;
	static {
		BLOCK_SIZE = CoreProperties.getLoader().getPropertyAsInteger(Page.class.getName() + ".BLOCK_SIZE", 4096);
	}

	/**
	 * Calculates the maximum number of bytes required to store a value of a
	 * particular {@link Type type} in disk.
	 * 
	 * @param type
	 *            the specified type
	 * @return the number of bytes required
	 */
	public static int maxSize(Type type) {
		return type.isFixedSize() ? type.maxSize() : ByteHelper.INT_SIZE + type.maxSize();
	}

	/**
	 * Calculates the number of bytes required to store a {@link Constant
	 * constant} in disk.
	 * 
	 * @param val
	 *            the specified value
	 * @return the number of bytes required
	 */
	public static int size(Constant val) {
		return val.getType().isFixedSize() ? val.size() : ByteHelper.INT_SIZE + val.size();
	}

	private IoBuffer contents = IoAllocator.newIoBuffer(BLOCK_SIZE);
	private FileMgr fileMgr = VanillaDb.fileMgr();

	/**
	 * Creates a new page. Although the constructor takes no arguments, it
	 * depends on a {@link FileMgr} object that it gets from the method
	 * {@link VanillaDb#fileMgr()}. That object is created during system
	 * initialization. Thus this constructor cannot be called until either
	 * {@link VanillaDb#init(String)} or {@link VanillaDb#initFileMgr(String)}
	 * or {@link VanillaDb#initFileAndLogMgr(String)} is called first.
	 */
	public Page() {
	}

	/**
	 * Populates the page with the contents of the specified disk block.
	 * 
	 * @param blk
	 *            a block ID
	 */
	public synchronized void read(BlockId blk) {
		fileMgr.read(blk, contents);
	}

	/**
	 * Writes the contents of the page to the specified disk block.
	 * 
	 * @param blk
	 *            a block ID
	 */
	public synchronized void write(BlockId blk) {
		fileMgr.write(blk, contents);
	}

	/**
	 * Appends the contents of the page to the specified file.
	 * 
	 * @param fileName
	 *            the name of the file
	 * 
	 * @return the reference to the newly-created disk block
	 */
	public synchronized BlockId append(String fileName) {
		return fileMgr.append(fileName, contents);
	}

	/**
	 * Returns the value at a specified offset of this page. If a constant was
	 * not stored at that offset, the behavior of the method is unpredictable.
	 * 
	 * @param offset
	 *            the byte offset within the page
	 * 
	 * @param type
	 *            the type of the value
	 * 
	 * @return the constant value at that offset
	 */
	public synchronized Constant getVal(int offset, Type type) {
		int size;
		byte[] byteVal = null;

		// Check the length of bytes
		if (type.isFixedSize()) {
			size = type.maxSize();
		} else {
			byteVal = new byte[ByteHelper.INT_SIZE];
			contents.get(offset, byteVal);
			size = ByteHelper.toInteger(byteVal);
			offset += ByteHelper.INT_SIZE;
		}

		// Get bytes and translate it to Constant
		byteVal = new byte[size];
		contents.get(offset, byteVal);
		return Constant.newInstance(type, byteVal);
	}

	/**
	 * Writes a constant value to the specified offset on the page.
	 * 
	 * @param offset
	 *            the byte offset within the page
	 * 
	 * @param val
	 *            the constant value to be written to the page
	 */
	public synchronized void setVal(int offset, Constant val) {
		byte[] byteval = val.asBytes();

		// Append the size of value if it is not fixed size
		if (!val.getType().isFixedSize()) {
			// check the field capacity and value size
			if (offset + ByteHelper.INT_SIZE + byteval.length > BLOCK_SIZE)
				throw new BufferOverflowException();

			byte[] sizeBytes = ByteHelper.toBytes(byteval.length);
			contents.put(offset, sizeBytes);
			offset += sizeBytes.length;
		}

		// Put bytes
		contents.put(offset, byteval);
	}

	/**
	 * Close this page to release resources.
	 */
	public void close() {
		contents.close();
	}
}
