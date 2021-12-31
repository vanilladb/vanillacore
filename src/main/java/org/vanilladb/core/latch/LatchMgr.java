package org.vanilladb.core.latch;

import java.util.HashMap;
import java.util.Map;

public class LatchMgr {
	Map<String, Latch> latchMap = new HashMap<String, Latch>();

	public ReentrantLatch registerReentrantLatch(String belonger, String function) {
		StringBuilder nameBuilder = new StringBuilder();
		nameBuilder.append(belonger);
		nameBuilder.append(function);
		
		ReentrantLatch latch = new ReentrantLatch();
		latchMap.put(nameBuilder.toString(), latch);
		return latch;
	}
	
	public ReentrantReadWriteLatch registerReentrantReadWriteLatch(String belonger, String function) {
		StringBuilder nameBuilder = new StringBuilder();
		nameBuilder.append(belonger);
		nameBuilder.append(function);
		
		ReentrantReadWriteLatch latch = new ReentrantReadWriteLatch();
		latchMap.put(nameBuilder.toString(), latch);
		return latch;
	}

	public void printLatchStats() {
		for (String name : latchMap.keySet()) {
			Latch latch = latchMap.get(name);
			System.out.println(
					String.format("%s, max: %d, tot: %d", name, latch.getMaxWaitingCount(), latch.getTotalAccessCount()));
		}
	}
}