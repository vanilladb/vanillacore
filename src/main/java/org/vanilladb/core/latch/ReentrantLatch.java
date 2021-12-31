package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantLock;

public class NormalLatch extends Latch{
	
	private ReentrantLock latch;
	
	public NormalLatch() {
		super();
		latch = new ReentrantLock();
	}
	
	public void lockLatch() {
		increaseWaitingCount();
		latch.lock();
	}

	public void unlockLatch() {
		decreaseWaitingCount();
		latch.unlock();
	}
	public boolean isHeldByCurrentThred() {
		return latch.isHeldByCurrentThread();
	}
}
