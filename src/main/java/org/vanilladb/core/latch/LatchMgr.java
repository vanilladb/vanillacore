package org.vanilladb.core.latch;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.latch.feature.LatchFeatureCollector;

public class LatchMgr {
//	private static Logger logger = Logger.getLogger(LatchMgr.class.getName());
	private static final String FEATURE_CSV = "latch-features.csv";
	protected LatchFeatureCollector collector = new LatchFeatureCollector(FEATURE_CSV);

	private Map<String, Latch> latchMap = new HashMap<String, Latch>();

	public static String getKey(String caller, String target, int index) {
		return caller + "-" + target + "-" + index;
	}

	public void startCollecting() {
		collector.startCollecting();
	}

	public ReentrantLatch registerReentrantLatch(String caller, String target, int index) {
		String key = getKey(caller, target, index);
		ReentrantLatch latch = new ReentrantLatch(key, collector);
		latchMap.put(key, latch);
		return latch;
	}

	public ReentrantReadWriteLatch registerReentrantReadWriteLatch(String caller, String target, int index) {
		String key = getKey(caller, target, index);
		ReentrantReadWriteLatch latch = new ReentrantReadWriteLatch(key, collector);
		latchMap.put(key, latch);
		return latch;
	}
}