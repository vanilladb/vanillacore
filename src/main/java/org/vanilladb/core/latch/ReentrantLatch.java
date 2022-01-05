package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.core.server.VanillaDb;

public class ReentrantLatch extends Latch {

	private ReentrantLock latch;

	public ReentrantLatch(String latchName) {
		super(latchName);
		latch = new ReentrantLock();
	}

	public void lockLatch() {
		LatchNote note = new LatchNote();
		noteMap.put(Thread.currentThread().getId(), note);
		takeNoteBeforeLock(note);
		
		recordStatsBeforeLock();
		latch.lock();
		takeNoteAfterLock(note);
	}

	public void unlockLatch() {
		latch.unlock();
		recordStatsAfterUnlock();
		LatchNote note = noteMap.get(Thread.currentThread().getId());
		VanillaDb.getLatchMgr().dispatchNoteToClerk(note);
	}

	public boolean isHeldByCurrentThread() {
		return latch.isHeldByCurrentThread();
	}
}
