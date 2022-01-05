package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.latch.context.LatchContext;

public class ReentrantReadWriteLatch extends Latch {

	private ReentrantReadWriteLock latch;

	public ReentrantReadWriteLatch(String latchName) {
		super(latchName);
		latch = new ReentrantReadWriteLock();
	}

	public void readLock() {
		LatchContext context = new LatchContext();
		contextMap.put(Thread.currentThread().getId(), context);
		setContextBeforeLock(context);
		recordStatsBeforeLock();
		latch.readLock().lock();
		setContextAfterLock(context);
	}

	public void writeLock() {
		LatchContext context = new LatchContext();
		contextMap.put(Thread.currentThread().getId(), context);
		setContextBeforeLock(context);
		recordStatsBeforeLock();
		latch.writeLock().lock();
		setContextAfterLock(context);
	}

	public void readUnlock() {
		// readUnlock
		latch.writeLock().unlock();
		recordStatsAfterUnlock();
		LatchContext context = contextMap.get(Thread.currentThread().getId());
		setContextAfterUnlock(context);
	}
	
	public void writeUnlock() {
		// writeUnlock
		latch.writeLock().unlock();
		recordStatsAfterUnlock();
		LatchContext context = contextMap.get(Thread.currentThread().getId());
		setContextAfterUnlock(context);
	}
}
