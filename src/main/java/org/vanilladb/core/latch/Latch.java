package org.vanilladb.core.latch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.vanilladb.core.latch.feature.LatchContext;
import org.vanilladb.core.latch.feature.LatchFeature;
import org.vanilladb.core.latch.feature.LatchHistory;
import org.vanilladb.core.server.VanillaDb;

public abstract class Latch {
	protected LatchHistory history = new LatchHistory();
	protected Map<Long, LatchContext> contextMap = new ConcurrentHashMap<Long, LatchContext>();
	private AtomicLong serialNumber = new AtomicLong();
	
	private String latchName;
	
	public Latch(String latchName) {
		this.latchName = latchName;
	}
	
	protected abstract int getQueueLength();

	public LatchFeature getFeature() {
		return new LatchFeature(latchName, getQueueLength(), history.toRow());
	}

	protected void setContextBeforeLock(LatchContext context) {
		context.setLatchName(latchName);
		context.setTimeBeforeLock();
		context.setSerialNumberBeforeLock(serialNumber.get());
		context.setWaitingQueueLength(getQueueLength());
	}

	protected void setContextAfterLock(LatchContext context) {
		context.setTimeAfterLock();
		context.setSerialNumberAfterLock(serialNumber.getAndIncrement());
	}

	protected void setContextAfterUnlock(LatchContext context) {
		context.setTimeAfterUnlock();
	}

	protected void addToHistory(LatchContext context) {
		history.addLatchContext(context);
	}

	protected Boolean isEnableCollecting() {
		return VanillaDb.isInited();
	}
}
