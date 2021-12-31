package org.vanilladb.core.latch;

public abstract class Latch {
	private int waitingCount;
	private int maxWaitingCount;
	private int totalAccessCount;

	public Latch() {
		waitingCount = 0;
		maxWaitingCount = 0;
		totalAccessCount = 0;
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

	protected synchronized void increaseWaitingCount() {
		waitingCount = waitingCount + 1;
		if (waitingCount > maxWaitingCount) {
			maxWaitingCount = waitingCount;
		}
		totalAccessCount = totalAccessCount + 1;
	}

	protected synchronized void decreaseWaitingCount() {
		waitingCount = waitingCount - 1;
	}
}
