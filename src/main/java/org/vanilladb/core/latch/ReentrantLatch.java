package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantLock;
import org.vanilladb.core.latch.context.LatchContext;
import org.vanilladb.core.server.VanillaDb;

public class ReentrantLatch extends Latch {
	private ReentrantLock latch = new ReentrantLock();

	public ReentrantLatch(String latchName) {
		super(latchName);
		latch = new ReentrantLock();
	}

	public void lock() {
		LatchContext context = new LatchContext();
		contextMap.put(Thread.currentThread().getId(), context);
		setContextBeforeLock(context);
		recordStatsBeforeLock();
		latch.lock();
		setContextAfterLock(context);
	}

	public void unlock() {
		latch.unlock();
		recordStatsAfterUnlock();
		LatchContext context = contextMap.get(Thread.currentThread().getId());
		setContextAfterUnlock(context);
		VanillaDb.getLatchMgr().dispatchContextToClerk(context);

	}

	public boolean isHeldByCurrentThread() {
		return latch.isHeldByCurrentThread();
	}
}
