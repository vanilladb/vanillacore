package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.latch.context.LatchContext;

public class ReentrantReadWriteLatch extends Latch {

	private ReentrantReadWriteLock latch;

	public ReentrantReadWriteLatch() {
		latch = new ReentrantReadWriteLock();
	}

	public void readLock(LatchContext context) {
		setContextBeforeLock(context, latch.getQueueLength());

		latch.readLock().lock();

		setContextAfterLock(context);
	}

	public void writeLock(LatchContext context) {
		setContextBeforeLock(context, latch.getQueueLength());

		latch.writeLock().lock();

		setContextAfterLock(context);
	}

	public void readUnlock(LatchContext context) {
		latch.readLock().unlock();

		setContextAfterUnlock(context);
	}

	public void writeUnlock(LatchContext context) {
		latch.writeLock().unlock();

		setContextAfterUnlock(context);
	}
}
