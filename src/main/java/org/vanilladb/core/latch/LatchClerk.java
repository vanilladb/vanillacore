package org.vanilladb.core.latch;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.vanilladb.core.latch.context.LatchContext;
import org.vanilladb.core.server.VanillaDb;

public class LatchClerk extends Thread {

	Queue<LatchContext> waitingContexts;
	

	public LatchClerk() {
		waitingContexts = new LinkedBlockingQueue<LatchContext>();
	}

	@Override
	public void run() {
		LatchMgr latchMgr = VanillaDb.getLatchMgr();
		while (true) {
			LatchContext context = waitingContexts.poll();
			while (context == null) continue;
			
			// clerk should add contexts to history when waitingContexts is not empty
			Latch latch = latchMgr.getLatchByName(context.getLatchName());
			latch.addContextToLatchHistory(context);
		}
	}

	public void addToWaitingContexts(LatchContext context) {
		waitingContexts.add(context);
	}
}
