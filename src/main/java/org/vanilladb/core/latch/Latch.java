package org.vanilladb.core.latch;

public abstract class Latch {
	private int waitingCount; 
	private int maxWaitingCount;
	
	public Latch(){
		waitingCount = 0;
		maxWaitingCount = 0;
	}
	
	public int getWaitingCount() {
		return waitingCount;
	}
	
	protected synchronized void increaseWaitingCount() {
		int curWaitingCount = waitingCount + 1;
		if (curWaitingCount > maxWaitingCount) {
			maxWaitingCount = curWaitingCount;
		}
	}
	
	protected synchronized void decreaseWaitingCount() {
		waitingCount = waitingCount - 1;
	}
}
