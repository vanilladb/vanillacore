package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantLock;

public class ReentrantLatch extends Latch {

	private ReentrantLock latch;

	public ReentrantLatch() {
		latch = new ReentrantLock();
	}

	public void lockLatch() {
		increaseWaitingCount();
		latch.lock();
	}

	public void unlockLatch() {
		latch.unlock();
		decreaseWaitingCount();
	}

	public boolean isHeldByCurrentThred() {
		return latch.isHeldByCurrentThread();
	}
}
