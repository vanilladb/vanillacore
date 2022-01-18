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
		if (isEnableCollecting()) {
			// create a brand new context to record the lock behavior
			LatchContext context = new LatchContext();

			// put the context into map or we cannot get the context in Unlock()
			contextMap.put(Thread.currentThread().getId(), context);

			// set the context with several feature that can be acquired before acquiring
			// lock
			setContextBeforeLock(context);

			latch.lock();

			// set the context with several feature that can be acquired after acquiring
			// lock
			setContextAfterLock(context);
		} else {
			// no need to keep contexts during the initialized state
			latch.lock();
		}
	}

	public void unlock() {
		latch.unlock();

		if (isEnableCollecting()) {
			// get the context from context map
			LatchContext context = contextMap.get(Thread.currentThread().getId());

			// set the context with several features that can be acquired after releasing
			// lock
			setContextAfterUnlock(context);

			// save the context to the history
			// imagine that history is just a type of context[10]
			addToHistory(context);

			// clean the contextMap
			contextMap.remove(Thread.currentThread().getId());
		}
	}

	public boolean isHeldByCurrentThread() {
		return latch.isHeldByCurrentThread();
	}

	@Override
	protected int getQueueLength() {
		return latch.getQueueLength();
	}
}
