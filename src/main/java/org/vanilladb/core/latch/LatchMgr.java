package org.vanilladb.core.latch;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.latch.context.LatchContext;

public class LatchMgr {
	private Map<String, Latch> latchMap = new HashMap<String, Latch>();
	private static final int MAX_LATCH_CLERKS_NUM = 10;
	private LatchClerk latchClerks [] = new LatchClerk[MAX_LATCH_CLERKS_NUM];;

	public LatchMgr() {
		for (int i=0; i<MAX_LATCH_CLERKS_NUM; i++) {
			latchClerks[i] = new LatchClerk();
		}
	}
	
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
	
	public void dispatchContextToClerk(LatchContext context) {
		 int clerkId = context.getLatchName().hashCode() % MAX_LATCH_CLERKS_NUM;
		 if (clerkId < 0)
			 clerkId = clerkId + MAX_LATCH_CLERKS_NUM;
		 
		 latchClerks[clerkId].addToWaitingContexts(context);
	}
	
	public void runLatchClerks() {
		for (int i=0; i<MAX_LATCH_CLERKS_NUM; i++) {
			latchClerks[i].start();
		}
	}

	public static String getKey(String caller, String target, int index) {
		return caller + target + index;
	}
}