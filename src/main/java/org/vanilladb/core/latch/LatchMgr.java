package org.vanilladb.core.latch;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LatchMgr {
	Map<String, Latch> latchMap = new HashMap<String, Latch>();
	
	public NormalLatch registerNormalLatch(String name) {
		NormalLatch normalLatch = new NormalLatch();
		latchMap.put(name, normalLatch);
		return normalLatch;
	}
	
	public ReadWriteLatch registerReadWriteLatch(String name) {
		ReadWriteLatch latch = new ReadWriteLatch();
		latchMap.put(name, latch);
		return latch;
	}
}