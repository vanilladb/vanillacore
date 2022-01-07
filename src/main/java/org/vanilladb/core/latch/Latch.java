package org.vanilladb.core.latch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.vanilladb.core.latch.context.LatchContext;

public abstract class Latch {
	private String name;

	private int waitingCount;
	private int maxWaitingCount;
	private int totalAccessCount;

	protected Map<Long, LatchContext> contextMap;
	protected Map<Long, String> historyMap;

	private LatchHistory history;

	public Latch(String latchName) {
		name = latchName;
		waitingCount = 0;
		maxWaitingCount = 0;
		totalAccessCount = 0;

		history = new LatchHistory();

		contextMap = new ConcurrentHashMap<Long, LatchContext>(); // Do I really need concurrent map?
		historyMap = new ConcurrentHashMap<Long, String>();
	}

	public void addContextToLatchHistory(LatchContext context) {
		history.addLatchContext(context);
	}

	public LatchHistory getHistory() {
		return history;
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

	protected void setContextBeforeLock(LatchContext context) {
		context.setTimeBeforeLock();
		context.setSerialNumberBeforeLock(totalAccessCount);
		context.setWaitingQueueLength(waitingCount);
	}

	protected void setContextAfterLock(LatchContext context) {
		context.setLatchName(name);
		context.setTimeAfterLock();
		context.setSerialNumberAfterLock(totalAccessCount);
	}

	protected void setContextAfterUnlock(LatchContext context) {
		context.setTimeAfterUnlock();
	}
}
