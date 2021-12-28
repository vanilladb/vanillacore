package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantLock;

public class NormalLatch extends Latch{
	
	private ReentrantLock latch;
	
	public NormalLatch() {
		super();
		latch = new ReentrantLock();
	}
	
	public void lockReadLatch() {
		increaseWaitingCount();
		latch.lock();
		
	}

	public void unlockWriteLatch() {
		decreaseWaitingCount();
		latch.unlock();
	}
}
