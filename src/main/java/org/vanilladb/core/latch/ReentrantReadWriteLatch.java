package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReentrantReadWriteLatch extends Latch {

	private ReentrantReadWriteLock latch;

	public ReentrantReadWriteLatch(String latchName) {
		super(latchName);
		latch = new ReentrantReadWriteLock();
	}

	public void lockReadLatch() {
		recordStatsBeforeLock();
		latch.readLock().lock();
	}

	public void lockWriteLatch() {
		recordStatsBeforeLock();
		latch.writeLock().lock();
	}

	public void unlockReadLatch() {
		latch.readLock().unlock();
		recordStatsAfterUnlock();
	}

	public void unlockWriteLatch() {
		latch.writeLock().unlock();
		recordStatsAfterUnlock();
	}
}
