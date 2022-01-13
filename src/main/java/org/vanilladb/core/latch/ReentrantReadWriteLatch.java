package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.latch.feature.LatchContext;
import org.vanilladb.core.latch.feature.ILatchFeatureCollector;

public class ReentrantReadWriteLatch extends Latch {

	private ReentrantReadWriteLock latch;

	public ReentrantReadWriteLatch(String latchName, ILatchFeatureCollector collector) {
		super(latchName, collector);
		latch = new ReentrantReadWriteLock();
	}

	public void readLock() {
		if (isEnableCollecting()) {
			LatchContext context = new LatchContext();
			contextMap.put(Thread.currentThread().getId(), context);

			setContextBeforeLock(context, latch.getQueueLength());
			latch.readLock().lock();
			setContextAfterLock(context);
		} else {
			latch.readLock().lock();
		}
	}

	public void writeLock() {
		if (isEnableCollecting()) {
			LatchContext context = new LatchContext();
			contextMap.put(Thread.currentThread().getId(), context);

			setContextBeforeLock(context, latch.getQueueLength());
			latch.writeLock().lock();
			setContextAfterLock(context);
		} else {
			latch.writeLock().lock();
		}
	}

	public void readUnlock() {
		// readUnlock
		latch.writeLock().unlock();

		if (isEnableCollecting()) {
			LatchContext context = contextMap.get(Thread.currentThread().getId());
			setContextAfterUnlock(context);
			addToHistory(context);
			addToCollector(context);
		}
	}

	public void writeUnlock() {
		// writeUnlock
		latch.writeLock().unlock();

		if (isEnableCollecting()) {
			LatchContext context = contextMap.get(Thread.currentThread().getId());
			setContextAfterUnlock(context);
			addToHistory(context);
			addToCollector(context);
		}
	}
}
