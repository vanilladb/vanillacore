package org.vanilladb.core.latch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Latch {
	private String name;
	
	private int waitingCount;
	private int maxWaitingCount;
	private int totalAccessCount;
	
	protected Map<Long, LatchNote> noteMap;
	private LatchHistory history;
	
	public Latch(String latchName) {
		name = latchName;
		waitingCount = 0;
		maxWaitingCount = 0;
		totalAccessCount = 0;
		
		noteMap = new ConcurrentHashMap<Long, LatchNote>(); // Do I really need concurrent map?
	}

	public int getWaitingCount() {
		return waitingCount;
	}

	public int getMaxWaitingCount() {
		return maxWaitingCount;
	}

	public int getTotalAccessCount() {
		return totalAccessCount;
	}

	protected synchronized void recordStatsBeforeLock() {
		waitingCount = waitingCount + 1;
		if (waitingCount > maxWaitingCount) {
			maxWaitingCount = waitingCount;
		}
	}

	protected synchronized void recordStatsAfterUnlock() {
		waitingCount = waitingCount - 1;
		totalAccessCount = totalAccessCount + 1;
	}
	
	protected void takeNoteBeforeLock(LatchNote note) {
		note.setLatchName(name);
		note.setWaitingQueueLength(waitingCount);
		note.setTimeBeforeLock();
		note.setSerialNumberBeforeLock(totalAccessCount);
	}
	
	protected void takeNoteAfterLock(LatchNote note) {
		note.setTimeAfterLock();
		note.setSerialNumberAfterLock(totalAccessCount);
	}
	
	protected void takeNoteAfterUnlock(LatchNote note) {
		note.setTimeAfterUnlock();
	}
	
	public void addNoteToLatchHistory(LatchNote note) {
		history.addLatchNote(note);
	}
	
	public LatchHistory getHistory() {
		return history;
	}
}
