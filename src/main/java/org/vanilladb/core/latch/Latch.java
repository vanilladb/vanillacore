package org.vanilladb.core.latch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.vanilladb.core.latch.feature.LatchContext;
import org.vanilladb.core.latch.feature.LatchFeature;
import org.vanilladb.core.latch.feature.LatchHistory;
import org.vanilladb.core.server.VanillaDb;

public abstract class Latch {
	protected LatchHistory history;
	protected Map<Long, String> historyMap;
	protected Map<Long, LatchContext> contextMap;
	protected AtomicReference<LatchFeature> currentFeature = new AtomicReference<LatchFeature>();

	private String latchName;
	private AtomicLong serialNumber;

	public Latch(String latchName) {
		this.latchName = latchName;
		serialNumber = new AtomicLong();

		history = new LatchHistory();

		historyMap = new ConcurrentHashMap<Long, String>();
		contextMap = new ConcurrentHashMap<Long, LatchContext>();
	}

	public LatchFeature getFeature() {
		return currentFeature.get();
	}

	public void snapshotHistory() {
		String historyString = history.toRow();
		historyMap.put(Thread.currentThread().getId(), historyString);
	}

	protected void setContextBeforeLock(LatchContext context, int queueLength) {
		context.setLatchName(latchName);
		context.setTimeBeforeLock();
		context.setSerialNumberBeforeLock(serialNumber.get());
		context.setWaitingQueueLength(queueLength);
	}

	protected void saveAsFeature(LatchContext context) {
		String historyString = historyMap.get(Thread.currentThread().getId());
		currentFeature.set(new LatchFeature(latchName, context, historyString));
		historyMap.remove(Thread.currentThread().getId());
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
