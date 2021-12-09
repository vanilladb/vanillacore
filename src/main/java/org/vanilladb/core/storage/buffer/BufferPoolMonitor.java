package org.vanilladb.core.storage.buffer;

public class BufferPoolMonitor {

	private static final int BUFFER_POOL_SIZE = BufferMgr.BUFFER_POOL_SIZE;
	
	private static final long[] readWaitCounters = new long[2000];
	private static final long[] writeWaitCounters = new long[2000];
	
	private static long lastTotalReadWaitCount;
	private static long lastTotalWriteWaitCount;
	
	public static double getAvgPinCount() {
		double totalPinCount = 0;
		for (Buffer buff : BufferMgr.bufferPool.buffers()) {
			totalPinCount += buff.getPinCount();
		}
    	return totalPinCount / BUFFER_POOL_SIZE;
	}
	
	public static int getPinnedBufferCount() {
		int available = BufferMgr.bufferPool.available();
		return BUFFER_POOL_SIZE - available;
	}
	
	public static double getHitRate() {
		return BufferMgr.bufferPool.hitRate();
	}
	
	public static int getReadWaitCount() {
		long count = 0;
		for (int i = 0; i < readWaitCounters.length; i++)
			count += readWaitCounters[i];
		long diff = count - lastTotalReadWaitCount;
		lastTotalReadWaitCount = count;
		return (int) diff;
	}

	public static int getWriteWaitCount() {
		long count = 0;
		for (int i = 0; i < writeWaitCounters.length; i++)
			count += writeWaitCounters[i];
		long diff = count - lastTotalWriteWaitCount;
		lastTotalWriteWaitCount = count;
		return (int) diff;
	}
	// modify
	public static int getBlockReleaseCount() {
		return BufferMgr.bufferPool.blockLockReleaseCount();
	}
	// modify
	public static int getBlockWaitCount() {
		return BufferMgr.bufferPool.blockLockWaitCount();
	}
	
	static void incrementReadWaitCounter() {
		int threadId = (int) Thread.currentThread().getId();
		readWaitCounters[threadId]++;
	}
	
	static void incrementWriteWaitCounter() {
		int threadId = (int) Thread.currentThread().getId();
		writeWaitCounters[threadId]++;
	}
}

