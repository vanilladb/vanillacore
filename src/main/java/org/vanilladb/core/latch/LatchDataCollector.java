package org.vanilladb.core.latch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.vanilladb.core.latch.context.LatchContext;
import org.vanilladb.core.latch.csv.CsvRow;
import org.vanilladb.core.latch.csv.CsvWriter;
import org.vanilladb.core.server.task.Task;

public class LatchDataCollector extends Task {
	private String name;
	private LinkedBlockingQueue<LatchFeature> latchFeatureQueue;
	private ArrayList<LatchFeature> latchFeatureList;

	private static final int Time_To_Flush = 20;

	public LatchDataCollector(String collectorName) {
		name = collectorName;
		latchFeatureQueue = new LinkedBlockingQueue<LatchFeature>();
		latchFeatureList = new ArrayList<LatchFeature>();
	}

	private static class LatchFeature implements CsvRow {
		private String contextString;
		private String historyString;

		LatchFeature(String contextString, String historyString) {
			this.contextString = contextString;
			this.historyString = historyString;
		}

		public static String toHeader() {
			return LatchContext.toHeader() + "," + LatchHistory.toHeader();
		}

		@Override
		public String toRow() {
			return contextString + "," + historyString;
		}
	}

	public void addLatchFeature(String contextString, String historyString) {
		latchFeatureQueue.add(new LatchFeature(contextString, historyString));
	}

	public void run() {
		try {
			// wait for first latch feature
			LatchFeature latchFeature = latchFeatureQueue.take();
			System.out.println("Latch feature collector starts to collect data");

			latchFeatureList.add(latchFeature);
			while ((latchFeature = latchFeatureQueue.poll(Time_To_Flush, TimeUnit.SECONDS)) != null) {
				latchFeatureList.add(latchFeature);
			}

			// Save to CSV
			saveToCsv();

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void saveToCsv() {
		CsvWriter<LatchFeature> csvWriter = new CsvWriter<LatchFeature>(name + ".csv");
		csvWriter.generateOutputFile(LatchFeature.toHeader(), latchFeatureList);
	}
}
