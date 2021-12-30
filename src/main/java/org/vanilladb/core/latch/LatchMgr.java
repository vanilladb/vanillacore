package org.vanilladb.core.latch;

import java.util.HashMap;
import java.util.Map;

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
	
	public void printLatchStats() {
		for (String name : latchMap.keySet()) {
			Latch latch = latchMap.get(name);
			System.out.println(String.format("%s, max: %d, tot: %d",
					name, latch.getMaxWaitingCount(), latch.getTotAccessCount()));
		}
	}
}