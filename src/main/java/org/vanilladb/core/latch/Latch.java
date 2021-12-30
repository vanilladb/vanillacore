package org.vanilladb.core.latch;

public abstract class Latch {
	private int waitingCount; 
	private int maxWaitingCount;
	private int totAccessCount;
	
	public Latch(){
		waitingCount = 0;
		maxWaitingCount = 0;
	}
	
	public int getWaitingCount() {
		return waitingCount;
	}
	
	public int getMaxWaitingCount() {
		return maxWaitingCount;
	}
	
	public int getTotAccessCount() {
		return totAccessCount;
	}
	
	protected synchronized void increaseWaitingCount() {
		waitingCount = waitingCount + 1;
		if (waitingCount > maxWaitingCount) {
			maxWaitingCount = waitingCount;
		}
		totAccessCount = totAccessCount + 1;
	}
	
	protected synchronized void decreaseWaitingCount() {
		waitingCount = waitingCount - 1;
	}
}
