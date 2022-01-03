package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.core.latch.context.LatchContext;

public class ReentrantLatch extends Latch {
	private ReentrantLock latch = new ReentrantLock();

	public void lock(LatchContext context) {
		setContextBeforeLock(context, latch.getQueueLength());

		latch.lock();

		setContextAfterLock(context);
	}

	public void unlock(LatchContext context) {
		latch.unlock();

		setContextAfterUnlock(context);
	}

	public boolean isHeldByCurrentThred() {
		return latch.isHeldByCurrentThread();
	}
}
