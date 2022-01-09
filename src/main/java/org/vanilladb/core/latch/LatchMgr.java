package org.vanilladb.core.latch;

import java.util.HashMap;
import java.util.Map;

public class LatchMgr {
	private Map<String, Latch> latchMap = new HashMap<String, Latch>();

	public static String getKey(String caller, String target, int index) {
		return caller + "-" + target + "-" + index;
	}

	public ReentrantLatch registerReentrantLatch(String caller, String target, int index) {
		String key = getKey(caller, target, index);
		ReentrantLatch latch = new ReentrantLatch(key);
		latchMap.put(key, latch);
		return latch;
	}

	public ReentrantReadWriteLatch registerReentrantReadWriteLatch(String caller, String target, int index) {
		String key = getKey(caller, target, index);
		ReentrantReadWriteLatch latch = new ReentrantReadWriteLatch(key);
		latchMap.put(key, latch);
		return latch;
	}
}