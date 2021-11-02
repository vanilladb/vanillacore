package org.vanilladb.core.storage.buffer;

public class BufferPoolMonitor {

	private final static int BUFFER_POOL_SIZE = BufferMgr.BUFFER_POOL_SIZE;
	
	public static double getAvgPinCount() {
		int totalPinCount = 0;
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

}
