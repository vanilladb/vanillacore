package org.vanilladb.core.latch;

import java.util.concurrent.atomic.AtomicLong;

import org.vanilladb.core.latch.context.LatchContext;

public abstract class Latch {
	private final AtomicLong serialNumber = new AtomicLong();

	protected void setContextBeforeLock(LatchContext context, int queueLength) {
		context.setTimeBeforeLock();
		context.setSerialNumberBeforeLock(serialNumber.get());
		context.setWaitingQueueLength(queueLength);
	}

	protected void setContextAfterLock(LatchContext context) {
		context.setTimeAfterLock();
		context.setSerialNumberAfterLock(serialNumber.get());
	}

	protected void setContextAfterUnlock(LatchContext context) {
		serialNumber.incrementAndGet();
		context.setTimeAfterUnlock();
	}
}
