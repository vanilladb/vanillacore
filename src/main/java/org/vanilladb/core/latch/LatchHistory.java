package org.vanilladb.core.latch;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.vanilladb.core.latch.context.LatchContext;
import org.vanilladb.core.latch.csv.CsvRow;

public class LatchHistory {
	private static final int MAX_HISTORY_LENGTH = 10;
	private Queue<LatchContext> historyQueue;

	public LatchHistory() {
		historyQueue = new LinkedBlockingQueue<LatchContext>();
		for (int i = 0; i < MAX_HISTORY_LENGTH; i++) {
			historyQueue.add(new LatchContext());
		}
	}

	public synchronized void addLatchContext(LatchContext context) {
		historyQueue.poll();
		historyQueue.add(context);
	}

	public synchronized String toRow() {
		String historyString = "";
		for (Object context : historyQueue.toArray()) {
			historyString += ((LatchContext) context).toRow() + ",";
		}
		return historyString;
	}

	public static String toHeader() {
		String header = "";
		for (int i = 0; i < MAX_HISTORY_LENGTH; i++) {
			header += LatchContext.toHeader(i) + ",";
		}
		return header;
	}
}
