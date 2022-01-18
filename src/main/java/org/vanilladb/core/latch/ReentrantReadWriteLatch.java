package org.vanilladb.core.latch;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReentrantReadWriteLatch extends Latch {

	private ReentrantReadWriteLock latch;

	public ReentrantReadWriteLatch(String latchName) {
		super(latchName);
		latch = new ReentrantReadWriteLock();
	}

	public void readLock() {
		// TODO: add feature collecting code
		latch.readLock().lock();
	}

	public void writeLock() {
		// TODO: add feature collecting code
		latch.writeLock().lock();
	}

	public void readUnlock() {
		// TODO: add feature collecting code
		latch.readLock().unlock();
	}

	public void writeUnlock() {
		// TODO: add feature collecting code
		latch.writeLock().unlock();
	}

	@Override
	protected int getQueueLength() {
		return 0;
	}
}
