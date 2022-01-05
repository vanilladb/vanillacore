package org.vanilladb.core.latch;

import java.util.HashMap;
import java.util.Map;

public class LatchMgr {
	private Map<String, Latch> latchMap = new HashMap<String, Latch>();
	private static final int MAX_LATCH_CLERKS_NUM = 10;
	private LatchClerk latchClerks [] = new LatchClerk[MAX_LATCH_CLERKS_NUM];;

	public LatchMgr() {
		for (int i=0; i<MAX_LATCH_CLERKS_NUM; i++) {
			latchClerks[i] = new LatchClerk();
		}
	}
	
	public ReentrantLatch registerReentrantLatch(String belonger, String function, int index) {
		String name = belonger + function + index;
		
		ReentrantLatch latch = new ReentrantLatch(name);
		latchMap.put(name, latch);
		return latch;
	}
	
	public ReentrantReadWriteLatch registerReentrantReadWriteLatch(String belonger, String function, int index) {
		String name = belonger + function + index;
		
		ReentrantReadWriteLatch latch = new ReentrantReadWriteLatch(name);
		latchMap.put(name, latch);
		return latch;
	}
	
	public Latch getLatchByName(String name) {
		return latchMap.get(name);
	}

	public void printLatchStats() {
		for (String name : latchMap.keySet()) {
			Latch latch = latchMap.get(name);
			System.out.println(
					String.format("%s, max: %d, tot: %d", name, latch.getMaxWaitingCount(), latch.getTotalAccessCount()));
		}
	}
	
	public void dispatchNoteToClerk(LatchNote note) {
		 int clerkId = note.getLatchName().hashCode() % MAX_LATCH_CLERKS_NUM;
		 if (clerkId < 0)
			 clerkId = clerkId + MAX_LATCH_CLERKS_NUM;
		 
		 latchClerks[clerkId].addToWaitingNotes(note);
	}
	
	public void runLatchClerks() {
		for (int i=0; i<MAX_LATCH_CLERKS_NUM; i++) {
			latchClerks[i].start();
		}
	}
}