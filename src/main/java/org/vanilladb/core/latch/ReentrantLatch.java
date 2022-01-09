package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.core.latch.feature.LatchContext;

public class ReentrantLatch extends Latch {
	private ReentrantLock latch = new ReentrantLock();

	public ReentrantLatch(String latchName) {
		super(latchName);
		latch = new ReentrantLock();
	}

	public void lock() {
		String historyString = history.toRow();
		historyMap.put(Thread.currentThread().getId(), historyString);

		LatchContext context = new LatchContext();
		contextMap.put(Thread.currentThread().getId(), context);

		setContextBeforeLock(context, latch.getQueueLength());
		latch.lock();
		setContextAfterLock(context);
	}

	public void unlock() {
		latch.unlock();

		LatchContext context = contextMap.get(Thread.currentThread().getId());
		setContextAfterUnlock(context);
		addToHistory(context);
		addToCollector(context);
	}

	public boolean isHeldByCurrentThread() {
		return latch.isHeldByCurrentThread();
	}
}
