/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.vanilladb.core.util;

public class Timers {

	private static final int THREAD_BOUND = 10000;

	private static TxComponentTimer[] timers = new TxComponentTimer[THREAD_BOUND];

	private static TxComponentTimer defaultTimer = new TxComponentTimer(-1);

	public static void createTimer(long txNum, Object... metadata) {
		int threadId = (int) Thread.currentThread().getId();
		timers[threadId] = new TxComponentTimer(txNum);
		timers[threadId].setMetadata(metadata);
	}

	public static TxComponentTimer getTimer() {
		int threadId = (int) Thread.currentThread().getId();

		if (timers[threadId] == null)
			return defaultTimer;

		return timers[threadId];
	}

	public static void reportTime() {
		int threadId = (int) Thread.currentThread().getId();

		String profile = timers[threadId].toString();

		if (profile != null && !profile.isEmpty())
			System.out.println(profile);

		timers[threadId] = null;
	}
}
