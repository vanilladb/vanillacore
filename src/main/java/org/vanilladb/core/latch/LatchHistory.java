package org.vanilladb.core.latch;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class LatchHistory {
	private static final int MAX_HISTORY_LENGTH = 10;
	private Queue<LatchNote> historyQueue;

	public LatchHistory() {
		historyQueue = new LinkedBlockingQueue<LatchNote>();
		for (int i = 0; i < MAX_HISTORY_LENGTH; i++) {
			historyQueue.add(new LatchNote());
		}
	}

	public synchronized void addLatchNote(LatchNote note) {
		historyQueue.poll();
		historyQueue.add(note);
	}
	
	public synchronized String toString() {
		String historyString = "";
		for (Object note: historyQueue.toArray()) {
			historyString += ((LatchNote) note).toString() + ",";
		}
		return historyString;
	}
}
