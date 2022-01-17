package org.vanilladb.core.latch.feature;

public class LatchFeature implements LatchFeatureRow {
	private String latchName;
	private long timeBeforeLock;
	private int queueLength;
	private String historyString;

	public LatchFeature(String latchName, LatchContext latchContext, String historyString) {
		this.latchName = latchName;
		this.timeBeforeLock = latchContext.getTimeBeforeLock();
		this.queueLength = latchContext.getWaitingQueueLength();
		this.historyString = historyString;
	}

	public static String toHeader() {
		return "latch name,time before lock,waiting queue length," + LatchHistory.toHeader();
	}

	public String getLatchName() {
		return latchName;
	}

	@Override
	public String toRow() {
		return latchName + "," + timeBeforeLock + "," + queueLength + "," + historyString;
	}
}
