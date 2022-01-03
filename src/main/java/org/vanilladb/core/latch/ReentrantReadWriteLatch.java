package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.latch.context.LatchContext;

public class ReentrantReadWriteLatch extends Latch {

	private ReentrantReadWriteLock latch;

	public ReentrantReadWriteLatch() {
		latch = new ReentrantReadWriteLock();
	}

	public void lockReadLatch(LatchContext context) {
		setContextBeforeLock(context, latch.getQueueLength());

		latch.readLock().lock();

		setContextAfterLock(context);
	}

	public void lockWriteLatch(LatchContext context) {
		setContextBeforeLock(context, latch.getQueueLength());

		latch.writeLock().lock();

		setContextAfterLock(context);
	}

	public void unlockReadLatch(LatchContext context) {
		latch.readLock().unlock();

		setContextAfterUnlock(context);
	}

	public void unlockWriteLatch(LatchContext context) {
		latch.writeLock().unlock();

		setContextAfterUnlock(context);
	}
}
