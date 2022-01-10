package org.vanilladb.core.latch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.vanilladb.core.latch.feature.LatchContext;
import org.vanilladb.core.latch.feature.LatchFeature;
import org.vanilladb.core.latch.feature.LatchFeatureCollector;
import org.vanilladb.core.latch.feature.LatchHistory;

public abstract class Latch {
	protected LatchHistory history;
	protected Map<Long, String> historyMap;
	protected Map<Long, LatchContext> contextMap;
	protected LatchFeatureCollector collector;

	private String name;
	private AtomicLong serialNumber;

	public Latch(String latchName, LatchFeatureCollector collector) {
		name = latchName;
		serialNumber = new AtomicLong();

		history = new LatchHistory();

		historyMap = new ConcurrentHashMap<Long, String>();
		contextMap = new ConcurrentHashMap<Long, LatchContext>();
		
		this.collector = collector;
	}

	protected void setContextBeforeLock(LatchContext context, long queueLength) {
		context.setLatchName(name);
		context.setTimeBeforeLock();
		context.setSerialNumberBeforeLock(serialNumber.get());
		context.setWaitingQueueLength(queueLength);
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

	protected void addToCollector(LatchContext context) {
		String historyString = historyMap.get(Thread.currentThread().getId());
		collector.addLatchFeature(new LatchFeature(context.toRow(), historyString));
	}
}
