package org.vanilladb.core.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class StripedLatchObserver<T> extends Task {
	public static final boolean ENABLE_OBSERVE_STRIPED_LOCK = false;

	private static final long TIME_TO_FLUSH = 10;
	private static Logger logger = Logger.getLogger(StripedLatchObserver.class.getName());
	private BlockingQueue<String> queue = new ArrayBlockingQueue<String>(1_000_000);
	private Map<Integer, Map<T, LatchInfo>> outerMap = new ConcurrentHashMap<Integer, Map<T, LatchInfo>>();
	private String fileName;

	private class LatchInfo {
		private int counter = 0;
		private int maxQueueLength = 0;

		public void counterIncrement() {
			counter += 1;
		}

		public void updateMaxQueueLength(int queueLength) {
			if (queueLength > maxQueueLength) {
				maxQueueLength = queueLength;
			}
		}

		public int getCounter() {
			return counter;
		}

		public int getMaxQueueLength() {
			return maxQueueLength;
		}
	}

	public StripedLatchObserver(String fileName) {
		this.fileName = fileName;
	}

	public void increment(int stripCode, T obj, int queueLength) {
		if (!VanillaDb.isInited()) {
			return;
		}

		if (!outerMap.containsKey(stripCode)) {
			outerMap.put(stripCode, new ConcurrentHashMap<T, LatchInfo>());
		}

		Map<T, LatchInfo> innerMap = outerMap.get(stripCode);

		if (!innerMap.containsKey(obj)) {
			innerMap.put(obj, new LatchInfo());
		}

		LatchInfo latchInfo = innerMap.get(obj);
		latchInfo.counterIncrement();
		latchInfo.updateMaxQueueLength(queueLength);

		queue.add(
				stripCode + ",\"" + obj + "\"," + latchInfo.getCounter() + "," + latchInfo.getMaxQueueLength() + "\n");
	}

	@Override
	public void run() {
		try {
			String row = queue.take();
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
				writer.append("strip code,obj,counter,max waiting queue length\n");
				writer.append(row);

				// Wait until no more statistics coming in the last 10 seconds
				while ((row = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
					writer.append(row);
				}
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}

			if (logger.isLoggable(Level.INFO)) {
				logger.info("save striped lock info to " + fileName);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
