package org.vanilladb.core.latch;

public enum LatchName {
	BUFFERPOOL_DATA_BLOCK("bufferPoolMgr", "dataBlock"), BUFFERPOOL_INDEX_BLOCK("bufferPoolMgr", "indexBlock");

	private String name;

	LatchName(String caller, String target) {
		this.name = caller + "-" + target;
	}

	public String getName() {
		return name;
	}
}
