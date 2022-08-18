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

	public static String getLatchKey(LatchName latchName, int index) {
		return latchName.getName() + "-" + index;
	}

	private static Map<String, Latch> latchMap = new HashMap<String, Latch>();

	public static String getKeyLatchFeatures() {
		String latchFeatures = getLatchFeature(LatchName.BUFFERPOOL_INDEX_BLOCK, 38) + ","
				+ getLatchFeature(LatchName.BUFFERPOOL_INDEX_BLOCK, 788);

		return latchFeatures;
	}

	private static String getLatchFeature(LatchName latchName, int index) {
		Latch latch = latchMap.get(getLatchKey(latchName, index));

		if (latch != null) {
			LatchFeature feature = latch.getFeature();
			if (feature != null) {
				return latch.getFeature().toRow();
			}
		}

		return "";
	}

	public static ReentrantLatch registerReentrantLatch(LatchName latchName, int index) {
		String latchKey = getLatchKey(latchName, index);

		ReentrantLatch latch = new ReentrantLatch(latchKey);

		latchMap.put(latchKey, latch);
		return latch;
	}

	public static ReentrantReadWriteLatch registerReentrantReadWriteLatch(LatchName latchName, int index) {
		String latchKey = getLatchKey(latchName, index);

		ReentrantReadWriteLatch latch = new ReentrantReadWriteLatch(latchKey);

		latchMap.put(latchKey, latch);
		return latch;
	}
}