package org.vanilladb.core.latch;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.vanilladb.core.latch.feature.ILatchFeatureCollector;

public class LatchMgr {
//	private static Logger logger = Logger.getLogger(LatchMgr.class.getName());

	/*
	 * Latches within the same stripped mechanism are managed by a single collector.
	 * caller + target maps to those latches. For example, 1009 latches, which
	 * consist of a stripped latch, are managed by a single collector.
	 */
	public static String getCollectorKey(String caller, String target) {
		return caller + "-" + target;
	}

	public static String getLatchKey(String caller, String target, int index) {
		return caller + "-" + target + "-" + index;
	}

	private Map<String, ILatchFeatureCollector> collectorMap;
	private Map<String, Latch> latchMap = new HashMap<String, Latch>();

	public LatchMgr(Map<String, ILatchFeatureCollector> collectorMap) {
		if (collectorMap != null) {
			this.collectorMap = collectorMap;
		} else {
			this.collectorMap = new HashMap<String, ILatchFeatureCollector>();
		}
		
		// start background threads to collect latch features
		for (Entry<String, ILatchFeatureCollector> entry: collectorMap.entrySet()) {
			entry.getValue().startCollecting();
		}
	}

	public ReentrantLatch registerReentrantLatch(String caller, String target, int index) {
		// NOTICE: DON'T handle the null collector
		ILatchFeatureCollector collector = collectorMap.get(getCollectorKey(caller, target));
		String latchKey = getLatchKey(caller, target, index);

		ReentrantLatch latch = new ReentrantLatch(latchKey, collector);

		latchMap.put(latchKey, latch);
		return latch;
	}

	public ReentrantReadWriteLatch registerReentrantReadWriteLatch(String caller, String target, int index) {
		// NOTICE: DON'T handle the null collector
		ILatchFeatureCollector collector = collectorMap.get(getCollectorKey(caller, target));
		String latchKey = getLatchKey(caller, target, index);

		ReentrantReadWriteLatch latch = new ReentrantReadWriteLatch(latchKey, collector);

		latchMap.put(latchKey, latch);
		return latch;
	}
}