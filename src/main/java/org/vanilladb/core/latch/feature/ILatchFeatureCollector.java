package org.vanilladb.core.latch.feature;

public interface ILatchFeatureCollector {
	public void startCollecting();
	public void addLatchFeature(LatchFeature latchFeature);
}
