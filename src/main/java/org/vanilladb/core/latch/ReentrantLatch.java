package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.core.latch.feature.LatchContext;
import org.vanilladb.core.latch.feature.ILatchFeatureCollector;

public class ReentrantLatch extends Latch {
	private ReentrantLock latch = new ReentrantLock();

	public ReentrantLatch(String latchName, ILatchFeatureCollector collector) {
		super(latchName, collector);
		latch = new ReentrantLock();
	}

	public void lock() {
		if (isEnableCollecting()) {
			String historyString = history.toRow();
			historyMap.put(Thread.currentThread().getId(), historyString);

			LatchContext context = new LatchContext();
			contextMap.put(Thread.currentThread().getId(), context);

			setContextBeforeLock(context, latch.getQueueLength());

			latch.lock();

			setContextAfterLock(context);
		} else {
			// no need to keep contexts during the initialized state
			latch.lock();
		}
	}

	public void unlock() {
		latch.unlock();

		if (isEnableCollecting()) {
			LatchContext context = contextMap.get(Thread.currentThread().getId());
			setContextAfterUnlock(context);
			addToHistory(context);
			addToCollector(context);
		}
	}

	public boolean isHeldByCurrentThread() {
		return latch.isHeldByCurrentThread();
	}
}
