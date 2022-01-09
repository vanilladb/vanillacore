package org.vanilladb.core.latch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.vanilladb.core.latch.feature.LatchContext;
import org.vanilladb.core.latch.feature.LatchFeature;
import org.vanilladb.core.latch.feature.LatchHistory;
import org.vanilladb.core.server.VanillaDb;

public abstract class Latch {
	protected static LatchDataCollector collector = new LatchDataCollector("latch-features");

	static {
		VanillaDb.taskMgr().runTask(collector);
	}

	protected LatchHistory history;
	protected Map<Long, String> historyMap;
	protected Map<Long, LatchContext> contextMap;

	private String name;
	private AtomicLong serialNumber;

	public Latch(String latchName) {
		name = latchName;
		serialNumber = new AtomicLong();

		history = new LatchHistory();

		historyMap = new ConcurrentHashMap<Long, String>();
		contextMap = new ConcurrentHashMap<Long, LatchContext>();
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
