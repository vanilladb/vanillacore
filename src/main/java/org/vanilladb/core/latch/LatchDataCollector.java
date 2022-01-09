package org.vanilladb.core.latch;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.latch.csv.CsvWriter;
import org.vanilladb.core.latch.feature.LatchFeature;
import org.vanilladb.core.server.task.Task;

public class LatchDataCollector extends Task {
	private static final int FLUSH_TIMEOUT = 20;
	private static Logger logger = Logger.getLogger(LatchDataCollector.class.getName());

	private String fileName;
	private LinkedBlockingQueue<LatchFeature> latchFeatureQueue;
	private ArrayList<LatchFeature> latchFeatureList;

	public LatchDataCollector(String fileName) {
		this.fileName = fileName;
		latchFeatureQueue = new LinkedBlockingQueue<LatchFeature>();
		latchFeatureList = new ArrayList<LatchFeature>();
	}

	public void addLatchFeature(LatchFeature latchFeature) {
		latchFeatureQueue.add(latchFeature);
	}

	public void run() {
		try {
			// wait for first latch feature
			LatchFeature latchFeature = latchFeatureQueue.take();
			if (logger.isLoggable(Level.INFO)) {
				logger.info("Latch feature collector starts to collect data");
			}

			latchFeatureList.add(latchFeature);
			while ((latchFeature = latchFeatureQueue.poll(FLUSH_TIMEOUT, TimeUnit.SECONDS)) != null) {
				latchFeatureList.add(latchFeature);
			}

			// Save to CSV
			saveToCsv();

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void saveToCsv() {
		CsvWriter<LatchFeature> csvWriter = new CsvWriter<LatchFeature>(fileName);
		csvWriter.generateOutputFile(LatchFeature.toHeader(), latchFeatureList);
	}
}
