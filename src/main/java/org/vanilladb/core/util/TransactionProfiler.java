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

public class TransactionProfiler {

	private static final String TOTAL_KEY = "Total";
	public static final boolean ENABLE_CPU_TIMER = true;
	public static final boolean ENABLE_DISKIO_COUNTER = true;

	private static final ThreadLocal<TransactionProfiler> LOCAL_PROFILER = new ThreadLocal<TransactionProfiler>() {
		@Override
		protected TransactionProfiler initialValue() {
			return new TransactionProfiler();
		}
	};

	/**
	 * Get the profiler local to this thread.
	 * 
	 * @return the local profiler
	 */
	public static TransactionProfiler getLocalProfiler() {
		return LOCAL_PROFILER.get();
	}
	
	public static TransactionProfiler takeOut() {
		TransactionProfiler profiler = LOCAL_PROFILER.get();
		LOCAL_PROFILER.set(new TransactionProfiler());
		return profiler;
	}
	
	public static void setProfiler(TransactionProfiler profiler) {
		LOCAL_PROFILER.set(profiler);
	}

	private static class SubProfiler {
		private int count = 0, startTimes = 0;
		private long timeStart = 0, totalTime = 0;
		private long cpuStart = 0, totalCpuTime = 0;
		private long ioStart = 0, totalIOCount = 0;
		private Thread currentThread;
		private boolean isCrossThreads = false;
		
		private SubProfiler() {
			// Do nothing
		}
		
		private SubProfiler(SubProfiler subProfiler) {
			this.count = subProfiler.count;
			this.startTimes = subProfiler.startTimes;
			this.timeStart = subProfiler.timeStart;
			this.totalTime = subProfiler.totalTime;
			this.cpuStart = subProfiler.cpuStart;
			this.totalCpuTime = subProfiler.totalCpuTime;
			this.ioStart = subProfiler.ioStart;
			this.totalIOCount = subProfiler.totalIOCount;
			this.currentThread = subProfiler.currentThread;
		}
		
		private void startProfiler(int ioStart) {
			if (startTimes == 0) {
				timeStart = System.nanoTime();
				if (ENABLE_CPU_TIMER) {
					currentThread = Thread.currentThread();
					cpuStart = ThreadMXBean.getCpuTime();
				}
				if (ENABLE_DISKIO_COUNTER)
					this.ioStart = ioStart;
			}
			startTimes++;
			count++;
		}

		private void stopProfiler(int ioStop) {
			startTimes--;
			if (startTimes == 0) {
				totalTime += (System.nanoTime() - timeStart) / 1000;
				if (ENABLE_CPU_TIMER) {
					checkCrossThreads();
					totalCpuTime = (ThreadMXBean.getCpuTime() - cpuStart) / 1000;
				}			
				if (ENABLE_DISKIO_COUNTER)
					this.totalIOCount = ioStop - ioStart;
			}
		}
		
		private long getTotalTime() {
			return totalTime;
		}
		
		private long getTotalCpuTime() {
			if (isCrossThreads)
				return -1;
			return totalCpuTime;
		}
		
		private long getTotalIOCount() {
			return totalIOCount;
		}

		private long getCount() {
			return count;
		}
		
		// Checking if a profiler is passed across threads
		private void checkCrossThreads() {	
			isCrossThreads = (currentThread != Thread.currentThread());
		}
	}

	private Map<Object, SubProfiler> subProfilers = new HashMap<Object, SubProfiler>();
	// We want to preserve the order of creating profilers so that
	// we use a list to record the order.
	private List<Object> components = new LinkedList<Object>();	
	private int ioCount = 0;

	public TransactionProfiler(TransactionProfiler profiler) {
		for (Map.Entry<Object, SubProfiler> subProfiler : profiler.subProfilers.entrySet())
			this.subProfilers.put(subProfiler.getKey(), new SubProfiler(subProfiler.getValue()));	
		this.components = new LinkedList<>(profiler.components);
		this.ioCount = profiler.ioCount;
	}
	
	private TransactionProfiler() {
		// Do nothing
	}

	public void reset() {
		subProfilers.clear();
		components.clear();
		ioCount = 0;
	}

	public void incrementIoCount() {
		ioCount += 1;
	}
	
	public void startComponentProfiler(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null) {
			profiler = new SubProfiler();
			subProfilers.put(component, profiler);
			components.add(component);
		}
		profiler.startProfiler(ioCount);
	}

	public void stopComponentProfiler(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler != null) {
			profiler.stopProfiler(ioCount);
		}
			
	}

	public long getComponentTime(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null)
			return -1;
		return profiler.getTotalTime();
	}

	public long getComponentCpuTime(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null)
			return -1;
		return profiler.getTotalCpuTime();
	}
	
	public long getComponentIOCount(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null)
			return -1;
		return profiler.getTotalIOCount();
	}

	public long getComponentCount(Object component) {
		return subProfilers.get(component).getCount();
	}

	public List<Object> getComponents() {
		return new LinkedList<Object>(components);
	}

	public void startExecution() {
		startComponentProfiler(TOTAL_KEY);
	}

	public void stopExecution() {
		stopComponentProfiler(TOTAL_KEY);
	}

	public long getExecutionTime() {
		return getComponentTime(TOTAL_KEY);
	}
	
	public long getTotalCpuTime() {
		return getComponentCpuTime(TOTAL_KEY);
	}
	
	public long getTotalIOCount() {
		return getComponentIOCount(TOTAL_KEY);
	}

	public void addToGlobalStatistics() {
		TimerStatistics.add(this);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("==============================\n");
		for (Object com : components) {
			if (!com.equals("Total")) {
				if(!ENABLE_CPU_TIMER && !ENABLE_DISKIO_COUNTER)
					sb.append(String.format("%-40s: %d us, with %d counts\n", com, subProfilers.get(com).getTotalTime(),
						subProfilers.get(com).getCount()));
				else
					sb.append(String.format("%-40s: %d us, %d us, %d times, with %d counts\n", com, subProfilers.get(com).getTotalTime(),
							subProfilers.get(com).getTotalCpuTime(), subProfilers.get(com).getTotalIOCount(), subProfilers.get(com).getCount()));
			}
		}
		if (subProfilers.get(TOTAL_KEY) != null) {
			if(!ENABLE_CPU_TIMER && !ENABLE_DISKIO_COUNTER)
				sb.append(String.format("%-40s: %d us\n", TOTAL_KEY, subProfilers.get(TOTAL_KEY).getTotalTime()));
			else
				sb.append(String.format("%-40s: %d us, %d us, %d times\n", TOTAL_KEY, subProfilers.get(TOTAL_KEY).getTotalTime(),
						subProfilers.get(TOTAL_KEY).getTotalCpuTime(), subProfilers.get(TOTAL_KEY).getTotalIOCount()));
		}
		sb.append("==============================\n");

		return sb.toString();
	}
}
