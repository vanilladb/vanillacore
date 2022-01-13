package org.vanilladb.core.latch.feature;

public class LatchFeature implements LatchRow {
	private String contextString;
	private String historyString;

	public LatchFeature(String contextString, String historyString) {
		this.contextString = contextString;
		this.historyString = historyString;
	}

	public static String toHeader() {
		return LatchContext.toHeader() + "," + LatchHistory.toHeader();
	}

	@Override
	public String toRow() {
		return contextString + "," + historyString;
	}
}
