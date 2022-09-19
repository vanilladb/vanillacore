package org.vanilladb.core.storage.tx.concurrency;

import java.util.concurrent.locks.ReentrantLock;

class LatchTable {

	private final ReentrantLock latches[] = new ReentrantLock[1009];
	
	public LatchTable() {
		for (int i = 0; i < latches.length; i++) {
			latches[i] = new ReentrantLock();
		}
	}
	
	public ReentrantLock getLatch(Object obj) {
		int code = Math.abs(obj.hashCode()); // avoid negative value
		code %= latches.length;
		
		return (ReentrantLock) latches[code];
	}
}