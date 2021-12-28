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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.core.latch.Latch;
import org.vanilladb.core.latch.LatchMgr;
import org.vanilladb.core.latch.NormalLatch;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.util.TransactionProfiler;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 */
class BufferPoolMgr {
	private Buffer[] bufferPool;
	private Map<BlockId, Buffer> blockMap;
	private volatile int lastReplacedBuff;
	private AtomicInteger numAvailable;
	
	private AtomicInteger totalCount;
	private AtomicInteger missCount;
	private AtomicInteger blockLockWaitCount;
	private AtomicInteger blockLockReleaseCount;

	// Optimization: Lock striping
//	private static final int stripSize = 1009;
//	private ReentrantLock[] fileLocks = new ReentrantLock[stripSize];
//	private ReentrantLock[] blockLocks = new ReentrantLock[stripSize];
	
	// Modify:
	private static final int stripSize = 1009;
	private NormalLatch[] fileLatches = new NormalLatch[stripSize];
	private NormalLatch[] blockLatches = new NormalLatch[stripSize];
	LatchMgr latchMgr = VanillaDb.getLatchMgr();

	private static Logger logger = Logger.getLogger(BufferMgr.class.getName());
	
	/**
	 * Creates a buffer manager having the specified number of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link org.vanilladb.core.storage.log.LogMgr LogMgr} objects that it gets
	 * from the class {@link VanillaDb}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link VanillaDb#initFileAndLogMgr(String)} or is called first.
	 * 
	 * @param numBuffs
	 *            the number of buffer slots to allocate. Must be at least 2.
	 */
	BufferPoolMgr(int numBuffs) {
		bufferPool = new Buffer[numBuffs];
		blockMap = new ConcurrentHashMap<BlockId, Buffer>(numBuffs);
		numAvailable = new AtomicInteger(numBuffs);
		totalCount = new AtomicInteger();
		missCount = new AtomicInteger();
		blockLockWaitCount = new AtomicInteger();
		blockLockReleaseCount = new AtomicInteger();
		
		lastReplacedBuff = 0;
		for (int i = 0; i < numBuffs; i++)
			bufferPool[i] = new Buffer();

//		for (int i = 0; i < stripSize; ++i) {
//			fileLocks[i] = new ReentrantLock();
//			blockLocks[i] = new ReentrantLock();
//		}
		// Modify:
		for (int i = 0; i < stripSize; ++i) {
			blockLatches[i] = latchMgr.registerNormalLatch("BufferMgr-blockLock-" + i);
			fileLatches[i] = latchMgr.registerNormalLatch("BufferMgr-fileLock-" + i);
		}
	}

//	// Optimization: Lock striping
//	private ReentrantLock prepareFileLock(Object o) {
//		int code = o.hashCode() % fileLocks.length;
//		if (code < 0)
//			code += fileLocks.length;
//		return fileLocks[code];
//	}
//	
//	// Optimization: Lock striping
//	private ReentrantLock prepareBlockLock(Object o) {
//		int code = o.hashCode() % blockLocks.length;
//		if (code < 0)
//			code += blockLocks.length;
//		return blockLocks[code];
//	}
	
	// Modify
	private NormalLatch prepareFileLatch(Object o) {
		int code = o.hashCode() % fileLatches.length;
		if (code < 0)
			code += fileLatches.length;
		return fileLatches[code];
	}
	
	private NormalLatch prepareBlockLatch(Object o) {
		int code = o.hashCode() % blockLatches.length;
		if (code < 0)
			code += blockLatches.length;
		return blockLatches[code];
	}

	/**
	 * Flushes all dirty buffers.
	 */
	void flushAll() {
		for (Buffer buff : bufferPool) {
			try {
				buff.getSwapLock().lock();
				buff.flush();
			} finally {
				buff.getSwapLock().unlock();
			}
		}
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a block ID
	 * @return the pinned buffer
	 */
	Buffer pin(BlockId blk) {
		// profiler
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		int stage = TransactionProfiler.getStageIndicator();
		String op = TransactionProfiler.getOperationIndicator();
		// The blockLock prevents race condition.
		// Only one tx can trigger the swapping action for the same block.
//		ReentrantLock blockLock = prepareBlockLock(blk);
		NormalLatch blockLatch = prepareBlockLatch(blk);
		
		profiler.startComponentProfiler(stage + op + "-BufferPoolMgr.pin blockLock");
		blockLockWaitCount.incrementAndGet();
		// Modify
//		blockLock.lock();
		blockLatch.lockLatch();
		blockLockWaitCount.decrementAndGet();
		profiler.stopComponentProfiler(stage + op + "-BufferPoolMgr.pin blockLock");

		try {
			// Find existing buffer
			Buffer buff = findExistingBuffer(blk);

			totalCount.incrementAndGet();
			// If there is no such buffer
			if (buff == null) {
				
				missCount.incrementAndGet();
				// Choose Unpinned Buffer
				int lastReplacedBuff = this.lastReplacedBuff;
				int currBlk = (lastReplacedBuff + 1) % bufferPool.length;
				// Note: this check will fail if there is only one buffer
				while (currBlk != lastReplacedBuff) {
					buff = bufferPool[currBlk];
					
					// Get the lock of buffer if it is free
					if (buff.getSwapLock().tryLock()) {
						try {
							// Check if there is no one use it
							if (!buff.isPinned() && !buff.checkRecentlyPinnedAndReset()) {
								this.lastReplacedBuff = currBlk;
								
								// Swap
								BlockId oldBlk = buff.block();
								if (oldBlk != null)
									blockMap.remove(oldBlk);
								buff.assignToBlock(blk);
								blockMap.put(blk, buff);
								if (!buff.isPinned())
									numAvailable.decrementAndGet();
								
								// Pin this buffer
								buff.pin();
								return buff;
							}
						} finally {
							// Release the lock of buffer
							buff.getSwapLock().unlock();
						}
					}
					currBlk = (currBlk + 1) % bufferPool.length;
				}
				return null;
				
			// If it exists
			} else {
				// Get the lock of buffer
				buff.getSwapLock().lock();
				
				// Optimization
				// Early release the blockLock
				// because the following txs, which need the same block, will get the same non-null buffer
				
				// Modify
//				blockLock.unlock();
				blockLatch.unlockLatch();
				blockLockReleaseCount.incrementAndGet();
				
				try {
					// Check its block id before pinning since it might be swapped
					if (buff.block().equals(blk)) {
						if (!buff.isPinned())
							numAvailable.decrementAndGet();
						buff.pin();
						return buff;
					}
					return pin(blk);
					
				} finally {
					// Release the lock of buffer
					buff.getSwapLock().unlock();
				}
			}
		} finally {
			// blockLock might be early released
			// unlocking a lock twice will get an exception 
//			if (blockLock.isHeldByCurrentThread()) {
//				blockLock.unlock();
//				blockLockReleaseCount.incrementAndGet();
//			}
			if (blockLatch.isHeldByCurrentThred()) {
				blockLatch.unlockLatch();
			}
		}
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	Buffer pinNew(String fileName, PageFormatter fmtr) {
		// Only the txs acquiring to append the block on the same file will be blocked
		// Modify
//		ReentrantLock fileLock = prepareFileLock(fileName);
//		fileLock.lock();
		NormalLatch fileLatch = prepareFileLatch(fileName);
		fileLatch.lockLatch();
		
		try {
			// Choose Unpinned Buffer
			int lastReplacedBuff = this.lastReplacedBuff;
			int currBlk = (lastReplacedBuff + 1) % bufferPool.length;
			while (currBlk != lastReplacedBuff) {
				Buffer buff = bufferPool[currBlk];
				
				// Get the lock of buffer if it is free
				if (buff.getSwapLock().tryLock()) {
					try {
						if (!buff.isPinned() && !buff.checkRecentlyPinnedAndReset()) {
							this.lastReplacedBuff = currBlk;
							
							// Swap
							BlockId oldBlk = buff.block();
							if (oldBlk != null)
								blockMap.remove(oldBlk);
							buff.assignToNew(fileName, fmtr);
							blockMap.put(buff.block(), buff);
							if (!buff.isPinned())
								numAvailable.decrementAndGet();
							
							// Pin this buffer
							buff.pin();
							return buff;
						}
					} finally {
						// Release the lock of buffer
						buff.getSwapLock().unlock();
					}
				}
				currBlk = (currBlk + 1) % bufferPool.length;
			}
			return null;
		} finally {
//			fileLock.unlock();
			// Modify
			fileLatch.unlockLatch();
		}
	}

	/**
	 * Unpins the specified buffers.
	 * 
	 * @param buffs
	 *            the buffers to be unpinned
	 */
	void unpin(Buffer... buffs) {
		// profiler
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		int stage = TransactionProfiler.getStageIndicator();
		String op = TransactionProfiler.getOperationIndicator();
		
		for (Buffer buff : buffs) {
			try {
				// Get the lock of buffer
				profiler.startComponentProfiler(stage + op + "-BufferPoolMgr.unpin swapLock");
				buff.getSwapLock().lock();
				profiler.stopComponentProfiler(stage + op + "-BufferPoolMgr.unpin swapLock");
				
				buff.unpin();
				if (!buff.isPinned())
					numAvailable.incrementAndGet();
			} finally {
				buff.getSwapLock().unlock();
			}
		}
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable.get();
	}

	private Buffer findExistingBuffer(BlockId blk) {
		return blockMap.get(blk);
	}
	
	Buffer[] buffers() {
		return bufferPool;
	}
	
	double hitRate() {
		int miss = missCount.getAndSet(0);
		int total = totalCount.getAndSet(0);
		if (total == 0)
			return 1.0;
		else
			return (1 - ((double) miss) / ((double) total));
	}
		
	int blockLockReleaseCount() {
		return blockLockReleaseCount.getAndSet(0);
	}
	
	int blockLockWaitCount() {
		return blockLockWaitCount.get();
	}
}
