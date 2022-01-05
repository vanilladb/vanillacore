package org.vanilladb.core.latch;

public class LatchNote {
	private long timeBeforeLock;
	private long timeAfterLock;
	private long timeAfterUnlock;

	private long serialNumberBeforeLock;
	private long serialNumberAfterLock;

	private long waitingQueueLength;
	
	private String latchName;

	public void setTimeBeforeLock() {
		timeBeforeLock = System.nanoTime();
	}

	public void setTimeAfterLock() {
		timeAfterLock = System.nanoTime();
	}

	public void setTimeAfterUnlock() {
		timeAfterUnlock = System.nanoTime();
	}

	public void setSerialNumberBeforeLock(long serialNumber) {
		serialNumberBeforeLock = serialNumber;
	}

	public void setSerialNumberAfterLock(long serialNumber) {
		serialNumberAfterLock = serialNumber;
	}

	public void setWaitingQueueLength(long queueLength) {
		waitingQueueLength = queueLength;
	}
	
	public void setLatchName(String name) {
		latchName = name;
	}
	
	public String getLatchName() {
		return latchName;
	}
	
	public String toString() {
		String noteString = "%d, %d, %d, %d";
		noteString = String.format(noteString, timeBeforeLock, timeAfterLock, timeAfterUnlock, 
				serialNumberBeforeLock - serialNumberAfterLock, waitingQueueLength);
		return noteString;
	}
}
