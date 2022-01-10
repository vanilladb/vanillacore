package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.core.latch.feature.LatchContext;
import org.vanilladb.core.latch.feature.LatchFeatureCollector;
import org.vanilladb.core.server.VanillaDb;

public class ReentrantLatch extends Latch {
	private ReentrantLock latch = new ReentrantLock();

	public ReentrantLatch(String latchName, LatchFeatureCollector collector) {
		super(latchName, collector);
		latch = new ReentrantLock();
	}

	public void lock() {
		if (VanillaDb.isInited()) {
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

		if (VanillaDb.isInited()) {
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
