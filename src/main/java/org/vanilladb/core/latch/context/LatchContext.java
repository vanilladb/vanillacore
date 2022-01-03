package org.vanilladb.core.latch.context;

public class LatchContext {
	private long timeBeforeLock;
	private long timeAfterLock;
	private long timeAfterUnlock;

	private long serialNumberBeforeLock;
	private long serialNumberAfterLock;

	private long waitingQueueLength;

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
	
	public String toHeader() {
		return "time before lock,time after lock,time after unlock,serial number before lock,serial number after lock,waiting queue length";
	}

	public String toHeader(int idx) {
		String s = "%d time before lock,%d time after lock,%d time after unlock,%d serial number before lock,%d serial number after lock,%d waiting queue length";
		s = String.format(s, idx, idx, idx, idx, idx, idx);
		return s;
	}

	public String toRow() {
		return timeBeforeLock + "," + timeAfterLock + "," + timeAfterUnlock + "," + serialNumberBeforeLock + ","
				+ serialNumberAfterLock + "," + waitingQueueLength;
	}
}
