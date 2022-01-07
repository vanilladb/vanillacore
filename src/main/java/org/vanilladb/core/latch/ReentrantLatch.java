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
		String historyString = getHistory().toRow();
		historyMap.put(Thread.currentThread().getId(), historyString);

		LatchContext context = new LatchContext();
		contextMap.put(Thread.currentThread().getId(), context);
		setContextBeforeLock(context);
		recordStatsBeforeLock();
		latch.lock();
		setContextAfterLock(context);
	}

//	public void unlock() {
//		latch.unlock();
//		recordStatsAfterUnlock();
//		LatchContext context = contextMap.get(Thread.currentThread().getId());
//		setContextAfterUnlock(context);
//		addContextToLatchHistory(context);
//	}

	public void unlock(LatchDataCollector collector) {
		latch.unlock();
		recordStatsAfterUnlock();
		LatchContext context = contextMap.get(Thread.currentThread().getId());
		setContextAfterUnlock(context);
		
		// add to history
		addContextToLatchHistory(context);

		// save to collector
		String historyString = historyMap.get(Thread.currentThread().getId());
		collector.addLatchFeature(context.toRow(), historyString);
	}

	public boolean isHeldByCurrentThread() {
		return latch.isHeldByCurrentThread();
	}
}
