package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReentrantReadWriteLatch extends Latch {

	private ReentrantReadWriteLock latch;

	public ReentrantReadWriteLatch() {
		latch = new ReentrantReadWriteLock();
	}

	public void lockReadLatch() {
		increaseWaitingCount();
		latch.readLock().lock();
	}

	public void lockWriteLatch() {
		increaseWaitingCount();
		latch.writeLock().lock();
	}

	public void unlockReadLatch() {
		latch.readLock().unlock();
		decreaseWaitingCount();
	}

	public void unlockWriteLatch() {
		latch.writeLock().unlock();
		decreaseWaitingCount();
	}
}
