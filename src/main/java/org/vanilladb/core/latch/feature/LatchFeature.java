package org.vanilladb.core.latch.feature;

public class LatchFeature implements LatchFeatureRow {
	private String latchName;
	private long observeTime;
	private int queueLength;
	private String historyString;

	public LatchFeature(String latchName, int queueLength, String historyString) {
		this.latchName = latchName;
		this.observeTime = System.nanoTime();
		this.queueLength = queueLength;
		this.historyString = historyString;
	}

	public static String toHeader() {
		return "latch name,observeTime,waiting queue length," + LatchHistory.toHeader();
	}

	public String getLatchName() {
		return latchName;
	}

	@Override
	public String toRow() {
		return latchName + "," + observeTime + "," + queueLength + "," + historyString;
	}
}
