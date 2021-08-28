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
	private static final boolean ENABLE_CPU_TIMER = true;
	private static final boolean ENABLE_DISKIO_COUNTER = true;

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

	private static class SubProfiler {
		private int count = 0, startTimes = 0;
		private long timeStart = 0, totalTime = 0;
		private long cpuStart = 0, totalCpuTime = 0;
		private long ioStart = 0, totalIOCount = 0;
		
		public void setStartProfile(long start) {
			if (startTimes == 0) 
				this.timeStart = start;
			startTimes++;
			count++;
		}
		
		public void setStartProfile(long start, long cpuStart, int ioStart) {
			if (startTimes == 0) {
				this.timeStart = start;
				this.cpuStart = cpuStart;
				if (ENABLE_DISKIO_COUNTER)
					this.ioStart = ioStart;
			}
			startTimes++;
			count++;
		}
		
		public void setStopProfile(long stop) {
			startTimes--;
			if (startTimes == 0)
				totalTime += (stop - cpuStart) / 1000;
		}
		
		public void setStopProfile(long stop, long cpuStop, int ioStop) {
			startTimes--;
			if (startTimes == 0) {
				totalTime += (stop - timeStart) / 1000;
				totalCpuTime += (cpuStop - cpuStart) / 1000;
				if (ENABLE_DISKIO_COUNTER)
					totalIOCount += ioStop - ioStart;
			}	
		}
		
		public void startProfiler(int ioStart) {
			if (startTimes == 0)
				timeStart = System.nanoTime();
			if (ENABLE_CPU_TIMER)
				cpuStart = ThreadMXBean.getCpuTime();
			if (ENABLE_DISKIO_COUNTER)
				this.ioStart = ioStart;	
			startTimes++;
			count++;
		}

		public void stopProfiler(int ioStop) {
			startTimes--;
			if (startTimes == 0) {
				totalTime += (System.nanoTime() - timeStart) / 1000;
				if (ENABLE_CPU_TIMER)
					totalCpuTime = (ThreadMXBean.getCpuTime()-cpuStart) / 1000;
				if (ENABLE_DISKIO_COUNTER)
					this.totalIOCount = ioStop - ioStart;
			}
		}

		public long getTotalTime() {
			return totalTime;
		}
		
		public long getTotalCpuTime() {
			return totalCpuTime;
		}
		
		public long getTotalIOCount() {
			return totalIOCount;
		}

		public long getCount() {
			return count;
		}
	}

	private Map<Object, SubProfiler> subProfilers = new HashMap<Object, SubProfiler>();
	// We want to preserve the order of creating profilers so that
	// we use a list to record the order.
	private List<Object> componenents = new LinkedList<Object>();	
	private int ioCount = 0;

	public void reset() {
		subProfilers.clear();
		componenents.clear();
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
			componenents.add(component);
		}
		profiler.startProfiler(ioCount);
	}
	
	public void startComponentProfiler(Object component, long startTime) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null) {
			profiler = new SubProfiler();
			subProfilers.put(component, profiler);
			componenents.add(component);
		}
		profiler.setStartProfile(startTime);
	}
	
	public void startComponentProfiler(Object component, long startTime, long cpuStartTime) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null) {
			profiler = new SubProfiler();
			subProfilers.put(component, profiler);
			componenents.add(component);
		}

		profiler.setStartProfile(startTime, cpuStartTime, ioCount);
	}

	public void stopComponentProfiler(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler != null)
			profiler.stopProfiler(ioCount);
	}
	
	public void stopComponentProfiler(Object component, long stopTime) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler != null)
			profiler.setStopProfile(stopTime);
	}

	public void stopComponentProfiler(Object component, long stopTime, long cpuStopTime) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler != null)
			profiler.setStopProfile(stopTime, cpuStopTime, ioCount);
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
		return new LinkedList<Object>(componenents);
	}
	
	public void setStartExecution(long start) {
		startComponentProfiler(TOTAL_KEY, start);
	}
	
	public void setStopExecution(long stop) {
		stopComponentProfiler(TOTAL_KEY, stop);
	}
	
	public void setStartExecution(long start, long cpuStart) {
		startComponentProfiler(TOTAL_KEY, start, cpuStart);
	}
	
	public void setStopExecution(long stop, long cpuStop) {
		stopComponentProfiler(TOTAL_KEY, stop, cpuStop);
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
		for (Object com : componenents) {
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
