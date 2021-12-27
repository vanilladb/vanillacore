package org.vanilladb.core.latch;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class latchMgr {
	Map<String, ReentrantLock> latchMap = new HashMap<String, ReentrantLock>();
	Map<String, ReentrantReadWriteLock> readWriteLatchMap = new HashMap<String, ReentrantReadWriteLock>();
	
	public ReentrantLock registerLatch(String name) {
		ReentrantLock latch = new ReentrantLock();
		latchMap.put(name, latch);
		return latch;
	}
	public ReentrantReadWriteLock registerReadWriteLatch(String name) {
		ReentrantReadWriteLock latch = new ReentrantReadWriteLock();
		readWriteLatchMap.put(name, latch);
		return latch;
	}
}