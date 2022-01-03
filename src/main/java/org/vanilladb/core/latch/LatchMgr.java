package org.vanilladb.core.latch;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.latch.context.LatchContext;

public class LatchMgr {
	Map<String, Latch> latchMap = new HashMap<String, Latch>();

	public static LatchContext newLatchContext() {
		return new LatchContext();
	}

	public ReentrantLatch registerReentrantLatch(String caller, String callerMethod) {
		ReentrantLatch latch = new ReentrantLatch();
		latchMap.put(getKey(caller, callerMethod), latch);
		return latch;
	}

	public ReentrantReadWriteLatch registerReentrantReadWriteLatch(String caller, String callerMethod) {
		ReentrantReadWriteLatch latch = new ReentrantReadWriteLatch();
		latchMap.put(getKey(caller, callerMethod), latch);
		return latch;
	}

	public String getKey(String caller, String callerMethod) {
		return caller + callerMethod;
	}
}