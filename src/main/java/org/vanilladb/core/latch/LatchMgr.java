package org.vanilladb.core.latch;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.latch.feature.LatchFeature;

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

	private static Map<String, Latch> latchMap = new HashMap<String, Latch>();

	public static String getKeyLatchFeatures() {
		String latchFeatures = getLatchFeature("BufferPoolMgr", "indexBlock", 38) + ","
				+ getLatchFeature("BufferPoolMgr", "indexBlock", 788);

		return latchFeatures;
	}

	private static String getLatchFeature(String caller, String target, int index) {
		Latch latch = latchMap.get(getLatchKey(caller, target, index));

		if (latch != null) {
			LatchFeature feature = latch.getFeature();
			if (feature != null) {
				return latch.getFeature().toRow();
			}
		}

		return "";
	}

	public static ReentrantLatch registerReentrantLatch(String caller, String target, int index) {
		String latchKey = getLatchKey(caller, target, index);

		ReentrantLatch latch = new ReentrantLatch(latchKey);

		latchMap.put(latchKey, latch);
		return latch;
	}

	public static ReentrantReadWriteLatch registerReentrantReadWriteLatch(String caller, String target, int index) {
		String latchKey = getLatchKey(caller, target, index);

		ReentrantReadWriteLatch latch = new ReentrantReadWriteLatch(latchKey);

		latchMap.put(latchKey, latch);
		return latch;
	}
}