package org.vanilladb.core.latch.feature;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class LatchFeatureCollector extends Task {
	private static final int FLUSH_TIMEOUT = 10;
	private static final int BUFFER_SIZE = 4096;
	private static Logger logger = Logger.getLogger(LatchFeatureCollector.class.getName());

	private String fileName;
	private BlockingQueue<LatchFeature> latchFeatures;

	public void startCollecting() {
		VanillaDb.taskMgr().runTask(this);
	}

	public LatchFeatureCollector(String fileName) {
		this.fileName = fileName;
		latchFeatures = new LinkedBlockingQueue<LatchFeature>();
	}

	public void addLatchFeature(LatchFeature latchFeature) {
		try {
			latchFeatures.put(latchFeature);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Latch Data Collector");
		try {
			// wait for first latch feature
			LatchFeature latchFeature = latchFeatures.take();
			if (logger.isLoggable(Level.INFO)) {
				logger.info("Latch feature collector starts to collect data");
			}

			try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName), BUFFER_SIZE)) {
				writer.append(LatchFeature.toHeader() + "\n");
				writer.append(latchFeature.toRow() + "\n");

				while ((latchFeature = latchFeatures.poll(FLUSH_TIMEOUT, TimeUnit.SECONDS)) != null) {
					writer.append(latchFeature.toRow() + "\n");
				}

				if (logger.isLoggable(Level.INFO)) {
					String log = String.format("%s is generated.", fileName);
					logger.info(log);
				}

			} catch (IOException e) {
				e.printStackTrace();
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
