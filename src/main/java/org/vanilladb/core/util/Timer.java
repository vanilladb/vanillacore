/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Timer {

	private static final String EXE_TIME_KEY = "Execution Time";

	private static final ThreadLocal<Timer> LOCAL_TIMER = new ThreadLocal<Timer>() {
		@Override
		protected Timer initialValue() {
			return new Timer();
		}
	};

	/**
	 * Get the timer local to this thread.
	 * 
	 * @return the local timer
	 */
	public static Timer getLocalTimer() {
		return LOCAL_TIMER.get();
	}

	private static class SubTimer {
		private long start = 0, startTimes = 0, totalTime = 0, count = 0;

		public void startTimer() {
			if (startTimes == 0)
				start = System.nanoTime();

			startTimes++;
			count++;
		}

		public void stopTimer() {
			startTimes--;

			if (startTimes == 0)
				totalTime += (System.nanoTime() - start) / 1000;
		}

		public long getTotalTime() {
			return totalTime;
		}

		public long getCount() {
			return count;
		}
	}

	private Map<Object, SubTimer> subTimers = new HashMap<Object, SubTimer>();
	// We want to preserve the order of creating timers so that
	// we use a list to record the order.
	private List<Object> componenents = new LinkedList<Object>();

	public void reset() {
		subTimers.clear();
		componenents.clear();
	}

	public void startComponentTimer(Object component) {
		SubTimer timer = subTimers.get(component);
		if (timer == null) {
			timer = new SubTimer();
			subTimers.put(component, timer);
			componenents.add(component);
		}
		timer.startTimer();
	}

	public void stopComponentTimer(Object component) {
		SubTimer timer = subTimers.get(component);
		if (timer != null)
			timer.stopTimer();
	}

	public long getComponentTime(Object component) {
		SubTimer timer = subTimers.get(component);
		if (timer == null)
			return -1;
		return timer.getTotalTime();
	}

	public long getComponentCount(Object component) {
		return subTimers.get(component).getCount();
	}

	public List<Object> getComponents() {
		return new LinkedList<Object>(componenents);
	}

	public void startExecution() {
		startComponentTimer(EXE_TIME_KEY);
	}

	public void stopExecution() {
		stopComponentTimer(EXE_TIME_KEY);
	}

	public long getExecutionTime() {
		return getComponentTime(EXE_TIME_KEY);
	}

	public void addToGlobalStatistics() {
		TimerStatistics.add(this);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("==============================\n");
		for (Object com : componenents) {
			if (!com.equals("Execution Time")) {
				sb.append(String.format("%-40s: %d us, with %d counts\n", com, subTimers.get(com).getTotalTime(),
						subTimers.get(com).getCount()));
			}
		}
		if (subTimers.get(EXE_TIME_KEY) != null)
			sb.append(String.format("%-40s: %d us\n", EXE_TIME_KEY, subTimers.get(EXE_TIME_KEY).getTotalTime()));
		sb.append("==============================\n");

		return sb.toString();
	}
}
