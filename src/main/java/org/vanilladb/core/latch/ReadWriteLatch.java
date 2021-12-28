package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteLatch extends Latch{
	
	private ReentrantReadWriteLock latch;
	
	public ReadWriteLatch() {
		super();
		latch = new ReentrantReadWriteLock();
	}
	
	public void lockReadLatch() {
		increaseWaitingCount();
		latch.readLock().lock();
	}
	
	public void lockWriteLatch() {
		increaseWaitingCount();
		latch.writeLock().lock();
	}
	
	public void unlockReadLatch () {
		decreaseWaitingCount();
		latch.readLock().unlock();
	}
	
	public void unlockWriteLatch() {
		decreaseWaitingCount();
		latch.writeLock().unlock();
	}
}
