package org.vanilladb.core.latch;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.vanilladb.core.server.VanillaDb;

public class LatchClerk extends Thread {

	Queue<LatchNote> waitingNotes;
	

	public LatchClerk() {
		waitingNotes = new LinkedBlockingQueue<LatchNote>();
	}

	@Override
	public void run() {
		LatchMgr latchMgr = VanillaDb.getLatchMgr();
		while (true) {
			LatchNote note = waitingNotes.poll();
			while (note == null) continue;
			
			// clerk should add notes to history when notes exist
			Latch latch = latchMgr.getLatchByName(note.getLatchName());
			latch.addNoteToLatchHistory(note);
		}
	}

	public void addToWaitingNotes(LatchNote note) {
		waitingNotes.add(note);
	}
}
