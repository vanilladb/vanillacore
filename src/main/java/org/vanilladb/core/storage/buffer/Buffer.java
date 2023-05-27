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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.log.LogSeqNum;

/**
 * An individual buffer. A buffer wraps a page and stores information about its
 * status, such as the disk block associated with the page, the number of times
 * the block has been pinned, whether the contents of the page have been
 * modified, and if so, the id of the modifying transaction and the LSN of the
 * corresponding log record.
 */
public class Buffer {
	
	/**
	 * The available size (in bytes) for a buffer. Besides the data from users,
	 * a buffer also puts some meta-data in front of them. The size of a buffer,
	 * therefore, is slightly smaller than the size of an physical block in storages. 
	 */
	public static final int BUFFER_SIZE = Page.BLOCK_SIZE - LogSeqNum.SIZE;
	
	private static final int LAST_LSN_OFFSET = 0;
	private static final int DATA_START_OFFSET = LogSeqNum.SIZE;
	
	private Page contents = new Page();
	private BlockId blk = null;
	private AtomicInteger pins = new AtomicInteger(0);
	private AtomicBoolean isRecentlyPinned = new AtomicBoolean(false);
	private boolean isNew = false;
	private boolean isModified = false;
	// TODO: We use (-1, -1) for the default value. Will this be a problem ?
	private LogSeqNum lastLsn = LogSeqNum.DEFAULT_VALUE;
	
	// Locks
	private final ReadWriteLock contentLock = new ReentrantReadWriteLock();
	private final Lock swapLock = new ReentrantLock();
	private final Lock flushLock = new ReentrantLock();
	
	/**
	 * Creates a new buffer, wrapping a new {@link Page page}. This constructor
	 * is called exclusively by the class {@link BasicBufferMgr}. It depends on
	 * the {@link org.vanilladb.core.storage.log.LogMgr LogMgr} object that it
	 * gets from the class {@link VanillaDb}. That object is created during
	 * system initialization. Thus this constructor cannot be called until
	 * {@link VanillaDb#initFileAndLogMgr(String)} or is called first.
	 */
	Buffer() {
	}

	/**
	 * Returns the value at the specified offset of this buffer's page. If an
	 * integer was not stored at that location, the behavior of the method is
	 * unpredictable.
	 * 
	 * @param offset
	 *            the byte offset of the page
	 * @param type
	 *            the type of the value
	 * 
	 * @return the constant value at that offset
	 */
	public Constant getVal(int offset, Type type) {
		contentLock.readLock().lock();
		try {
			if (offset < 0 || offset >= BUFFER_SIZE)
				throw new IndexOutOfBoundsException("" + offset);
		
			return contents.getVal(DATA_START_OFFSET + offset, type);
		} finally {
			contentLock.readLock().unlock();
		}
	}
	
	void setVal(int offset, Constant val) {
		contentLock.writeLock().lock();
		try {
			if (offset < 0 || offset >= BUFFER_SIZE)
				throw new IndexOutOfBoundsException("" + offset);

			contents.setVal(DATA_START_OFFSET + offset, val);
		} finally {
			contentLock.writeLock().unlock();
		}
	}

	/**
	 * Writes a value to the specified offset of this buffer's page. This method
	 * assumes that the transaction has already written an appropriate log
	 * record. The buffer saves the id of the transaction and the LSN of the log
	 * record. A negative lsn value indicates that a log record was not
	 * necessary.
	 * 
	 * @param offset
	 *            the byte offset within the page
	 * @param val
	 *            the new value to be written
	 * @param txNum
	 *            the id of the transaction performing the modification
	 * @param lsn
	 *            the LSN of the corresponding log record
	 */
	public void setVal(int offset, Constant val, long txNum, LogSeqNum lsn) {
		contentLock.writeLock().lock();
		try {
			if (offset < 0 || offset >= BUFFER_SIZE)
				throw new IndexOutOfBoundsException("" + offset);
			
			isModified = true;
			
			if (lsn != null && lsn.compareTo(lastLsn) > 0)
				lastLsn = lsn;
			
			if (lsn != null) {
				// Put the last LSN in front of the data
				lastLsn.writeToPage(contents, LAST_LSN_OFFSET);				
			}
			
			contents.setVal(DATA_START_OFFSET + offset, val);
		} finally {
			contentLock.writeLock().unlock();
		}
	}
	
	/**
	 * Return the log sequence number (LSN) of the latest log record 
	 * which has been applied to this buffer. Note that the last LSN
	 * might be {@code null}.
	 * 
	 * @return the LSN of the latest affected log record
	 */
	public LogSeqNum lastLsn(){
		// Use contentLock because lastLsn will be modified from setVal.
		contentLock.readLock().lock();
		try {
			return lastLsn;
		} finally {
			contentLock.readLock().unlock();
		}
	}

	/**
	 * Returns a block ID refers to the disk block that the buffer is pinned to. <br><br>
	 * 
	 * <b>Warning:</b><br>
	 * Always make sure to get buff.block() before unpin.<br>
	 * Or you may get an unexpected block.
	 * 
	 * @return a block ID
	 */
	public BlockId block() {
		// Optimization
		// blk will be modified only if no txs pin this buffer 
		return blk;
	}
	
	/**
	 * Lock the flushing mechanism in order to prevent a thread
	 * flushing this buffer while another thread is doing a 
	 * physiological operation.
	 * 
	 * @see Buffer#unlockFlushing()
	 */
	public void lockFlushing() {
		flushLock.lock();
	}
	
	/**
	 * Unlock the flushing mechanism to make a buffer be able to
	 * be flushed by other thread. Note that the thread unlocking
	 * this mechanism must be the same thread as the one locking.
	 * 
	 * @see Buffer#lockFlushing()
	 */
	public void unlockFlushing() {
		flushLock.unlock();
	}
	
	protected Lock getSwapLock() {
		return swapLock;
	}

	protected void close() {
		contentLock.writeLock().lock();
		try {
			contents.close();
		} finally {
			contentLock.writeLock().unlock();
		}
	}

	/**
	 * Writes the page to its disk block if the page is dirty. The method
	 * ensures that the corresponding log record has been written to disk prior
	 * to writing the page to disk.
	 */
	void flush() {
		contentLock.writeLock().lock();
		flushLock.lock();
		try {
			if (isNew || isModified) {
				VanillaDb.logMgr().flush(lastLsn);
				contents.write(blk);
				isModified = false;
				isNew = false;
			}
		} finally {
			flushLock.unlock();
			contentLock.writeLock().unlock();
		}
	}

	/**
	 * Increases the buffer's pin count.
	 */
	void pin() {
		// Optimization: This might be a danger optimization
		// We have to make sure that txs have acquired swapLock before pin(),
		// so that no two txs can call pin at the same time.
		pins.incrementAndGet();
		isRecentlyPinned.set(true);
	}

	/**
	 * Decreases the buffer's pin count.
	 */
	void unpin() {
		// Optimization: This might be a danger optimization
		// We have to make sure that txs have acquired swapLock before unpin(),
		// so that no two txs can call unpin at the same time.
		pins.decrementAndGet();
	}

	/**
	 * Returns true if the buffer is currently pinned (that is, if it has a
	 * nonzero pin count).
	 * 
	 * @return true if the buffer is pinned
	 */
	boolean isPinned() {
		// Optimization: This might be a danger optimization
		// We have to make sure that txs have acquired swapLock before isPinned(),
		// so that no two txs can call isPinned at the same time.
		return pins.get() > 0;
	}
	
	boolean checkRecentlyPinnedAndReset() {
		return isRecentlyPinned.getAndSet(false);
	}

	/**
	 * Returns true if the buffer is dirty due to a modification.
	 * 
	 * @return true if the buffer is dirty
	 */
	boolean isModified() {
		contentLock.writeLock().lock();
		try {
			return isModified;
		} finally {
			contentLock.writeLock().unlock();
		}
	}

	/**
	 * Reads the contents of the specified block into the buffer's page. If the
	 * buffer was dirty, then the contents of the previous page are first
	 * written to disk.
	 * 
	 * @param blk
	 *            a block ID
	 */
	void assignToBlock(BlockId blk) {
		// Optimization: This might be a danger optimization
		// This method is called because no tx pin this buffer,
		// which means no tx will modify or read the content.
		if (pins.get() > 0) {
			throw new RuntimeException("The buffer is pinned by other transactions");
		}
		
		flush();
		this.blk = blk;
		contents.read(blk);
		pins.set(0);
		lastLsn = LogSeqNum.readFromPage(contents, LAST_LSN_OFFSET);
	}

	/**
	 * Initializes the buffer's page according to the specified formatter, and
	 * appends the page to the specified file. If the buffer was dirty, then the
	 * contents of the previous page are first written to disk.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a page formatter, used to initialize the page
	 */
	void assignToNew(String fileName, PageFormatter fmtr) {
		// Optimization: This might be a dangerous optimization
		// This method is called because no tx is pinning this buffer,
		// which means no tx will modify or read the content.
		if (pins.get() > 0) {
			throw new RuntimeException("The buffer is pinned by other transactions");
		}
		
		flush();
		fmtr.format(this);
		blk = contents.append(fileName);
		pins.set(0);
		isNew = true;
		lastLsn = LogSeqNum.DEFAULT_VALUE;
	}
	
	/**
	 * This method is designed for debugging.
	 * 
	 * @return the underlying page
	 */
	Page getUnderlyingPage() {
		return contents;
	}
	
	public int getPinCount() {
		return pins.get();
	}
}
