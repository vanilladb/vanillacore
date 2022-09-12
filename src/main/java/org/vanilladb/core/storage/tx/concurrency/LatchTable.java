package org.vanilladb.core.storage.tx.concurrency;

import java.util.concurrent.locks.ReentrantLock;

class LatchTable {

	private final ReentrantLock fhpLatches[] = new ReentrantLock[1009];
	
	public LatchTable() {
		for (int i = 0; i < fhpLatches.length; i++) {
			fhpLatches[i] = new ReentrantLock();
		}
	}
	
	public ReentrantLock getFhpLatch(Object obj) {
		int code = Math.abs(obj.hashCode()); // avoid negative value
		code %= fhpLatches.length;
		
		return (ReentrantLock) fhpLatches[code];
	}
}