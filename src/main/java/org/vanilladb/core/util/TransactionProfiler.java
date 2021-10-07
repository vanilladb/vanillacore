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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TransactionProfiler {

	private static final String TOTAL_KEY = "Total";
	public static final boolean ENABLE_CPU_TIMER = true;
	public static final boolean ENABLE_DISKIO_COUNTER = true;
	public static final boolean ENABLE_NETWORKIO_COUNTER = true;
	
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
	
	public static int getMessageSize(Object object) throws IOException {
	    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
	         ObjectOutputStream out = new ObjectOutputStream(bos)) {
	        out.writeObject(object);
	        return bos.size();
	    } 
	}

	private static class SubProfiler {
		private int invocationCount = 0, startCount = 0;
		private long elapsedTimeStart = 0, totalElapsedTime = 0;
		private long cpuTimeStart = 0, totalCpuTime = 0;
		private long diskIOCountStart = 0, totalDiskIOCount = 0;
		private long networkInSizeStart = 0, totalNetworkInSize = 0;
		private long networkOutSizeStart = 0, totalNetworkOutSize = 0;
		
		private Thread currentThread;
		private boolean isJumppingBetweenThreads = false;
		
		private SubProfiler() {
		// Do nothing
		}
		
		private SubProfiler(SubProfiler subProfiler) {
			this.invocationCount = subProfiler.invocationCount;
			this.startCount = subProfiler.startCount;
			this.elapsedTimeStart = subProfiler.elapsedTimeStart;
			this.totalElapsedTime = subProfiler.totalElapsedTime;
			this.cpuTimeStart = subProfiler.cpuTimeStart;
			this.totalCpuTime = subProfiler.totalCpuTime;
			this.diskIOCountStart = subProfiler.diskIOCountStart;
			this.totalDiskIOCount = subProfiler.totalDiskIOCount;
			this.totalNetworkInSize = subProfiler.totalNetworkInSize;
			this.networkInSizeStart = subProfiler.networkInSizeStart;
			this.networkOutSizeStart = subProfiler.networkOutSizeStart;
			this.totalNetworkOutSize = subProfiler.totalNetworkOutSize;
			this.currentThread = subProfiler.currentThread;
		}
		
		private void startProfiler(int diskIOStart, int networkInStart, int networkOutStart) {
			if (startCount == 0) {
				elapsedTimeStart = System.nanoTime();
				if (ENABLE_CPU_TIMER) {
					currentThread = Thread.currentThread();
					cpuTimeStart = ThreadMXBean.getCpuTime();
				}
				if (ENABLE_DISKIO_COUNTER)
					this.diskIOCountStart = diskIOStart;
				if (ENABLE_NETWORKIO_COUNTER) {
					this.networkInSizeStart = networkInStart;
					this.networkOutSizeStart = networkOutStart;
				}
			}
			startCount++;
			invocationCount++;
		}

		private void stopProfiler(int ioStop, int networkInStop, int networkOutStop) {
			startCount--;
			if (startCount == 0) {
				totalElapsedTime += (System.nanoTime() - elapsedTimeStart) / 1000;
				if (ENABLE_CPU_TIMER) {
					checkCrossThreads();
					totalCpuTime = (ThreadMXBean.getCpuTime() - cpuTimeStart) / 1000;
				}			
				if (ENABLE_DISKIO_COUNTER)
					this.totalDiskIOCount = ioStop - diskIOCountStart;
				if (ENABLE_NETWORKIO_COUNTER) {
					this.totalNetworkInSize = networkInStop - networkInSizeStart;
					this.totalNetworkOutSize = networkOutStop - networkOutSizeStart;
				}
			}
		}
		
		private void addProfile(long elapasedTime, long cpuTime,
				long diskIOCount, long networkInSize, long networkOutSize, int invocationCount) {
			this.totalElapsedTime += elapasedTime;
			this.totalCpuTime += cpuTime;
			this.totalDiskIOCount += diskIOCount;
			this.totalNetworkInSize += networkInSize;
			this.totalNetworkOutSize += networkOutSize;
			this.invocationCount += invocationCount;
		}

		
		private long getTotalElapsedTime() {
			return totalElapsedTime;
		}
		
		private long getTotalCpuTime() {
			if (isJumppingBetweenThreads)
				return -1;
			return totalCpuTime;
		}
		
		private long getTotalDiskIOCount() {
			return totalDiskIOCount;
		}
		
		private long getNetworkInSize() {
			return totalNetworkInSize;
		}
		
		private long getNetworkOutSize() {
			return totalNetworkOutSize;
		}

		private int getInvocationCount() {
			return invocationCount;
		}
		
		// Checking if a profiler is passed across threads
		private void checkCrossThreads() {	
			isJumppingBetweenThreads = (currentThread != Thread.currentThread());
		}
	}

	private Map<Object, SubProfiler> subProfilers = new HashMap<Object, SubProfiler>();
	// We want to preserve the order of creating profilers so that
	// we use a list to record the order.
	private List<Object> components = new LinkedList<Object>();	
	private int diskIOCount = 0;
	private int networkInSize = 0;
	private int networkOutSize = 0;

	public TransactionProfiler(TransactionProfiler profiler) {
		for (Map.Entry<Object, SubProfiler> subProfiler : profiler.subProfilers.entrySet())
			this.subProfilers.put(subProfiler.getKey(), new SubProfiler(subProfiler.getValue()));	
		this.components = new LinkedList<Object>(profiler.components);
		this.diskIOCount = profiler.diskIOCount;
		this.networkInSize = profiler.networkInSize;
		this.networkOutSize = profiler.networkOutSize;
	}
	
	private TransactionProfiler() {
		// Do nothing
	}

	public void reset() {
		subProfilers.clear();
		components.clear();
		diskIOCount = 0;
		networkInSize = 0;
		networkOutSize = 0;
	}

	public void incrementDiskIOCount() {
		diskIOCount += 1;
	}
	
	public void incrementNetworkInSize(Serializable object) {
		try {
			networkInSize += getMessageSize(object);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void incrementNetworkOutSize(Serializable object) {
		try {
			networkOutSize += getMessageSize(object);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private SubProfiler getOrNewSubProfiler(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null) {
			profiler = new SubProfiler();
			subProfilers.put(component, profiler);
			components.add(component);
		}
		return profiler;
	}
	
	public void addComponentProfile(Object component, long elapasedTime, long cpuTime,
			long diskIOCount, long networkIOInSize, long networkIOOutSize, int invocationCount) {
		SubProfiler profiler = getOrNewSubProfiler(component);
		profiler.addProfile(elapasedTime, cpuTime, diskIOCount, networkIOInSize, networkIOOutSize, invocationCount);
	}
	
	public void startComponentProfiler(Object component) {
		SubProfiler profiler = getOrNewSubProfiler(component);
		profiler.startProfiler(diskIOCount, networkInSize, networkOutSize);
	}

	public void stopComponentProfiler(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler != null) {
			profiler.stopProfiler(diskIOCount, networkInSize, networkOutSize);		
		}
	}

	public long getComponentTime(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null)
			return -1;
		return profiler.getTotalElapsedTime();
	}

	public long getComponentCpuTime(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null)
			return -1;
		return profiler.getTotalCpuTime();
	}
	
	public long getComponentDiskIOCount(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null)
			return -1;
		return profiler.getTotalDiskIOCount();
	}
	
	public long getComponentNetworkInSize(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null)
			return -1;
		return profiler.getNetworkInSize();
	}
	
	public long getComponentNetworkOutSize(Object component) {
		SubProfiler profiler = subProfilers.get(component);
		if (profiler == null)
			return -1;
		return profiler.getNetworkOutSize();
	}

	public int getComponentCount(Object component) {
		return subProfilers.get(component).getInvocationCount();
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
	
	public long getTotalDiskIOCount() {
		return getComponentDiskIOCount(TOTAL_KEY);
	}
	
	public long getNetworkInSize() {
		return getComponentNetworkInSize(TOTAL_KEY);
	}
	
	public long getNetworkOutSize() {
		return getComponentNetworkOutSize(TOTAL_KEY);
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
					sb.append(String.format("%-40s: %d us, with %d counts\n", com, subProfilers.get(com).getTotalElapsedTime(),
						subProfilers.get(com).getInvocationCount()));
				else
					sb.append(String.format("%-40s: %d us, %d us, %d times, with %d counts\n", com, subProfilers.get(com).getTotalElapsedTime(),
							subProfilers.get(com).getTotalCpuTime(), subProfilers.get(com).getTotalDiskIOCount(), subProfilers.get(com).getInvocationCount()));
			}
		}
		if (subProfilers.get(TOTAL_KEY) != null) {
			if(!ENABLE_CPU_TIMER && !ENABLE_DISKIO_COUNTER)
				sb.append(String.format("%-40s: %d us\n", TOTAL_KEY, subProfilers.get(TOTAL_KEY).getTotalElapsedTime()));
			else
				sb.append(String.format("%-40s: %d us, %d us, %d times\n", TOTAL_KEY, subProfilers.get(TOTAL_KEY).getTotalElapsedTime(),
						subProfilers.get(TOTAL_KEY).getTotalCpuTime(), subProfilers.get(TOTAL_KEY).getTotalDiskIOCount()));
		}
		sb.append("==============================\n");

		return sb.toString();
	}
}
