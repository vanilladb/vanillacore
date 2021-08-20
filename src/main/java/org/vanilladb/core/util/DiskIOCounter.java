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

public class DiskIOCounter {

	private static final String TOTAL_COUNT_KEY = "Total I/O";

	private static final ThreadLocal<DiskIOCounter> LOCAL_IO_COUNTER = new ThreadLocal<DiskIOCounter>() {
		@Override
		protected DiskIOCounter initialValue() {
			return new DiskIOCounter();
		}
	};

	/**
	 * Get the IO counter local to this thread.
	 * 
	 * @return the local timer
	 */
	public static DiskIOCounter getLocalIOCounter() {
		return LOCAL_IO_COUNTER.get();
	}

	private static class SubCounter {
		private int start = 0, startCounts = 0, totalCount = 0, count = 0;
		
		public void startCounter(int start) {
			if (startCounts == 0)
				this.start = start;

			startCounts++;
			count++;
		}

		public void stopCounter(int stop) {
			startCounts--;

			if (startCounts == 0)
				totalCount += (stop - start);
		}

		public long getTotalCount() {
			return totalCount;
		}

		public long getCount() {
			return count;
		}
	}

	private Map<Object, SubCounter> subCounters = new HashMap<Object, SubCounter>();
	// We want to preserve the order of creating timers so that
	// we use a list to record the order.
	private List<Object> componenents = new LinkedList<Object>();
	private int count = 0;
	
	public void reset() {
		subCounters.clear();
		componenents.clear();
	}
	
	public void add() {
		count += 1;
	}
	
	public int getCount() {
		return count;
	}

	public void startComponentCounter(Object component) {
		SubCounter counter = subCounters.get(component);
		if (counter == null) {
			counter = new SubCounter();
			subCounters.put(component, counter);
			componenents.add(component);
		}
		counter.startCounter(count);
	}

	public void stopComponentCounter(Object component) {
		SubCounter counter = subCounters.get(component);
		if (counter != null)
			counter.stopCounter(count);
	}

	public long getComponentIOCount(Object component) {
		SubCounter counter = subCounters.get(component);
		if (counter == null)
			return -1;
		return counter.getTotalCount();
	}

	public long getComponentCount(Object component) {
		return subCounters.get(component).getCount();
	}

	public List<Object> getComponents() {
		return new LinkedList<Object>(componenents);
	}

	public void start() {
		startComponentCounter(TOTAL_COUNT_KEY);
	}

	public void stop() {
		stopComponentCounter(TOTAL_COUNT_KEY);
	}

	public long getTotalIOCount() {
		return getComponentCount(TOTAL_COUNT_KEY);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("==============================\n");
		for (Object com : componenents) {
			if (!com.equals("Total I/O")) {
				sb.append(String.format("%-40s: %d us, with %d counts\n", com, subCounters.get(com).getTotalCount(),
						subCounters.get(com).getCount()));
			}
		}
		if (subCounters.get(TOTAL_COUNT_KEY) != null)
			sb.append(String.format("%-40s: %d us\n", TOTAL_COUNT_KEY, subCounters.get(TOTAL_COUNT_KEY).getTotalCount()));
		sb.append("==============================\n");

		return sb.toString();
	}
}
