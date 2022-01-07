package org.vanilladb.core.latch;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.latch.context.LatchContext;

public class LatchMgr {
	private Map<String, Latch> latchMap = new HashMap<String, Latch>();

	public LatchMgr() {}
	
	public ReentrantLatch registerReentrantLatch(String caller, String target, int index) {
		String name = getKey(caller, target, index);
		ReentrantLatch latch = new ReentrantLatch(name);
		latchMap.put(name, latch);
		return latch;
	}
	
	public ReentrantReadWriteLatch registerReentrantReadWriteLatch(String caller, String target, int index) {
		String key = getKey(caller, target, index);
		ReentrantReadWriteLatch latch = new ReentrantReadWriteLatch(key);
		latchMap.put(key, latch);
		return latch;
	}
	
	public Latch getLatchByName(String name) {
		return latchMap.get(name);
	}

	public void printLatchStats() {
		for (String key : latchMap.keySet()) {
			Latch latch = latchMap.get(key);
			System.out.println(
					String.format("%s, max: %d, tot: %d", key, latch.getMaxWaitingCount(), latch.getTotalAccessCount()));
		}
	}

	public static String getKey(String caller, String target, int index) {
		return caller + target + index;
	}

}