package org.vanilladb.core.latch.feature;

import org.vanilladb.core.latch.csv.CsvRow;

public class LatchFeature implements CsvRow {
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
