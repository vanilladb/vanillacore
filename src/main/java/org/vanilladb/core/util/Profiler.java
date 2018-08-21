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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * A non-agent-based CPU profiler similar to
 * 
 * <pre>
 * <code>java -agentlib:hprof=cpu=samples ToBeProfiledClass</code>
 * </pre>
 * 
 * but accepts filters and can be started/stopped at any time.
 */
public class Profiler implements Runnable {
	private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");
	private static final int INTERVAL;
	private static final int DEPTH;
	private static final int MAX_PACKAGES;
	private static final int MAX_METHODS;
	private static final int MAX_LINES;
	private static final String[] IGNORE_THREADS;
	private static final String[] IGNORE_PACKAGES;

	static {
		INTERVAL = CoreProperties.getLoader().getPropertyAsInteger(Profiler.class.getName() + ".INTERVAL", 10);
		DEPTH = CoreProperties.getLoader().getPropertyAsInteger(Profiler.class.getName() + ".DEPTH", 5);
		MAX_PACKAGES = CoreProperties.getLoader().getPropertyAsInteger(Profiler.class.getName() + ".MAX_PACKAGES", 100);
		MAX_METHODS = CoreProperties.getLoader().getPropertyAsInteger(Profiler.class.getName() + ".MAX_METHODS", 1000);
		MAX_LINES = CoreProperties.getLoader().getPropertyAsInteger(Profiler.class.getName() + ".MAX_LINES", 1000);

		String[] defaultIgnoreThreads = new String[] { "java.lang.Thread.dumpThreads", "java.lang.Thread.getThreads",
				"java.net.PlainSocketImpl.socketAccept", "java.net.SocketInputStream.socketRead0",
				"java.net.SocketOutputStream.socketWrite0", "java.lang.UNIXProcess.waitForProcessExit",
				// "java.lang.Object.wait", "java.lang.Thread.sleep",
				"un.awt.windows.WToolkit.eventLoop", "sun.misc.Unsafe.park",
				"dalvik.system.VMStack.getThreadStackTrace", "dalvik.system.NativeStart.run" };
		IGNORE_THREADS = CoreProperties.getLoader()
				.getPropertyAsStringArray(Profiler.class.getName() + ".IGNORE_THREADS", defaultIgnoreThreads);

		String[] defaultIgnorePackages = new String[] { "java.", "javax.", "sun.", "net." };
		IGNORE_PACKAGES = CoreProperties.getLoader()
				.getPropertyAsStringArray(Profiler.class.getName() + ".IGNORE_PACKAGES", defaultIgnorePackages);
	}

	private Thread thread;
	private boolean started;
	private boolean paused;
	private long time;
	private long pauseTime;

	private CountMap<String> packages;
	private CountMap<String> selfMethods;
	private CountMap<String> stackMethods;
	private CountMap<String> lines;
	private int total;

	/**
	 * Start collecting profiling data.
	 */
	public synchronized void startCollecting() {
		paused = false;
		if (thread != null)
			return;

		packages = new CountMap<String>(MAX_PACKAGES);
		selfMethods = new CountMap<String>(MAX_METHODS);
		stackMethods = new CountMap<String>(MAX_METHODS);
		lines = new CountMap<String>(MAX_LINES);
		total = 0;

		started = true;
		thread = new Thread(this);
		thread.setName("Profiler");
		thread.setDaemon(true);
		thread.start();
	}

	/**
	 * Stop collecting.
	 */
	public synchronized void stopCollecting() {
		started = false;
		if (thread != null) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// ignore
			}
			thread = null;
		}
	}

	/**
	 * Pause collecting.
	 */
	public synchronized void pauseCollecting() {
		paused = true;
	}

	@Override
	public void run() {
		time = System.nanoTime();
		while (started) {
			try {
				tick();
			} catch (Exception ex) {
				ex.printStackTrace();
				break;
			}
		}
		time = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time);
	}

	private void tick() {
		long ts = System.nanoTime();
		if (INTERVAL > 0) {
			try {
				Thread.sleep(INTERVAL);
			} catch (Exception e) {
				// ignore
			}
		}
		if (paused) {
			pauseTime += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ts);
			return;
		}
		Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
		for (Map.Entry<Thread, StackTraceElement[]> entry : map.entrySet()) {
			Thread t = entry.getKey();
			if (t.getState() != Thread.State.RUNNABLE) {
				continue;
			}
			StackTraceElement[] trace = entry.getValue();
			if (trace == null || trace.length == 0) {
				continue;
			}
			if (startsWithAny(trace[0].toString(), IGNORE_THREADS)) {
				continue;
			}
			boolean relevant = tickPackages(trace);
			tickMethods(trace);
			tickLines(trace);
			if (relevant)
				total++;
		}
	}

	private boolean tickPackages(StackTraceElement[] trace) {
		for (int i = 0; i < trace.length; i++) {
			StackTraceElement el = trace[i];
			if (!startsWithAny(el.getClassName(), IGNORE_PACKAGES)) {
				String packageName = getPackageName(el);
				packages.increment(packageName);
				return true;
			}
		}
		return false;
	}

	private void tickMethods(StackTraceElement[] trace) {
		boolean self = true;
		StackTraceElement last = null;
		for (int i = 0; i < trace.length; i++) {
			StackTraceElement el = trace[i];
			if (!startsWithAny(el.getClassName(), IGNORE_PACKAGES)) {
				// ignore direct recursive calls
				if (last == null || !(el.getClassName().equals(last.getClassName())
						&& el.getMethodName().equals(last.getMethodName()))) {
					last = el;
					String methodName = getMethodName(el);
					if (self) {
						selfMethods.increment(methodName);
						self = false;
					}
					stackMethods.increment(methodName);
				}
			}
		}
	}

	private void tickLines(StackTraceElement[] trace) {
		StringBuilder buff = new StringBuilder();
		StackTraceElement last = null;
		for (int i = 0, j = 0; i < trace.length && j < DEPTH; i++) {
			StackTraceElement el = trace[i];
			// ignore direct recursive calls
			if (last == null || !(el.getClassName().equals(last.getClassName())
					&& el.getMethodName().equals(last.getMethodName()))) {
				if (!startsWithAny(el.getClassName(), IGNORE_PACKAGES)) {
					if (last == null) {
						buff.append(el.toString()).append("+").append(LINE_SEPARATOR);
					} else if (startsWithAny(last.getClassName(), IGNORE_PACKAGES)) {
						buff.append(last.toString()).append(LINE_SEPARATOR);
						buff.append(el.toString()).append("+").append(LINE_SEPARATOR);
					} else {
						buff.append(el.toString()).append(LINE_SEPARATOR);
					}
					j++;
				}
				last = el;
			}
		}
		if (buff.length() > 0)
			lines.increment(buff.toString().trim());
	}

	private static boolean startsWithAny(String s, String[] prefixes) {
		for (String p : prefixes) {
			if (p.length() > 0 && s.startsWith(p)) {
				return true;
			}
		}
		return false;
	}

	private static String getPackageName(StackTraceElement el) {
		String className = el.getClassName();
		int ci = className.lastIndexOf('.');
		if (ci > 0)
			return className.substring(0, ci);
		throw new IllegalArgumentException();
	}

	private static String getMethodName(StackTraceElement el) {
		return el.getClassName() + "." + el.getMethodName();
	}

	/**
	 * Stop and obtain the top packages, ordered by their self execution time.
	 * 
	 * @param num
	 *            number of top packages
	 * @return the top packages
	 */
	public String getTopPackages(int num) {
		stopCollecting();
		CountMap<String> pkgs = new CountMap<String>(packages);
		StringBuilder buff = new StringBuilder();
		buff.append("Top packages over ").append(time).append(" ms (").append(pauseTime).append(" ms paused), with ")
				.append(total).append(" counts:").append(LINE_SEPARATOR);
		buff.append("Rank\tSelf\tPackage").append(LINE_SEPARATOR);
		for (int i = 0, n = 0; pkgs.size() > 0 && n < num; i++) {
			int highest = 0;
			List<Map.Entry<String, Integer>> bests = new ArrayList<Map.Entry<String, Integer>>();
			for (Map.Entry<String, Integer> el : pkgs.entrySet()) {
				if (el.getValue() > highest) {
					bests.clear();
					bests.add(el);
					highest = el.getValue();
				} else if (el.getValue() == highest) {
					bests.add(el);
				}
			}
			for (Map.Entry<String, Integer> e : bests) {
				pkgs.remove(e.getKey());
				int percent = 100 * highest / Math.max(total, 1);
				buff.append(i + 1).append("\t").append(percent).append("%\t").append(e.getKey()).append(LINE_SEPARATOR);
				n++;
			}
		}
		return buff.toString();
	}

	/**
	 * Stop and obtain the self execution time of packages, each as a row in CSV
	 * format.
	 * 
	 * @return the execution time of packages in CSV format
	 */
	public String getPackageCsv() {
		stopCollecting();
		StringBuilder buff = new StringBuilder();
		buff.append("Package,Self").append(LINE_SEPARATOR);
		for (String k : new TreeSet<String>(packages.keySet())) {
			int percent = 100 * packages.get(k) / Math.max(total, 1);
			buff.append(k).append(",").append(percent).append(LINE_SEPARATOR);
		}
		return buff.toString();
	}

	/**
	 * Stop and obtain the top methods, ordered by their self execution time.
	 * 
	 * @param num
	 *            number of top methods
	 * 
	 * @return the top methods
	 */
	public String getTopMethods(int num) {
		stopCollecting();
		CountMap<String> selfms = new CountMap<String>(selfMethods);
		CountMap<String> stackms = new CountMap<String>(stackMethods);
		StringBuilder buff = new StringBuilder();
		buff.append("Top methods over ").append(time).append(" ms (").append(pauseTime).append(" ms paused), with ")
				.append(total).append(" counts:").append(LINE_SEPARATOR);
		buff.append("Rank\tSelf\tStack\tMethod").append(LINE_SEPARATOR);
		for (int i = 0, n = 0; selfms.size() > 0 && n < num; i++) {
			int highest = 0;
			List<Map.Entry<String, Integer>> bests = new ArrayList<Map.Entry<String, Integer>>();
			for (Map.Entry<String, Integer> el : selfms.entrySet()) {
				if (el.getValue() > highest) {
					bests.clear();
					bests.add(el);
					highest = el.getValue();
				} else if (el.getValue() == highest) {
					bests.add(el);
				}
			}
			for (Map.Entry<String, Integer> e : bests) {
				selfms.remove(e.getKey());
				int selfPercent = 100 * highest / Math.max(total, 1);
				int stackPercent = 100 * stackms.remove(e.getKey()) / Math.max(total, 1);
				buff.append(i + 1).append("\t").append(selfPercent).append("%\t").append(stackPercent).append("%\t")
						.append(e.getKey()).append(LINE_SEPARATOR);
				n++;
			}
		}
		return buff.toString();
	}

	/**
	 * Stop and obtain the self execution time of methods, each as a row in CSV
	 * format.
	 * 
	 * @return the execution time of methods in CSV format
	 */
	public String getMethodCsv() {
		stopCollecting();
		StringBuilder buff = new StringBuilder();
		buff.append("Method,Self").append(LINE_SEPARATOR);
		for (String k : new TreeSet<String>(selfMethods.keySet())) {
			int percent = 100 * selfMethods.get(k) / Math.max(total, 1);
			buff.append(k).append(",").append(percent).append(LINE_SEPARATOR);
		}
		return buff.toString();
	}

	/**
	 * Stop and obtain the top lines, ordered by their execution time.
	 * 
	 * @param num
	 *            number of top lines
	 * 
	 * @return the top lines
	 */
	public String getTopLines(int num) {
		stopCollecting();
		CountMap<String> ls = new CountMap<String>(lines);
		StringBuilder buff = new StringBuilder();
		buff.append("Top lines over ").append(time).append(" ms (").append(pauseTime).append(" ms paused), with ")
				.append(total).append(" counts:").append(LINE_SEPARATOR);
		for (int i = 0, n = 0; ls.size() > 0 && n < num; i++) {
			int highest = 0;
			List<Map.Entry<String, Integer>> bests = new ArrayList<Map.Entry<String, Integer>>();
			for (Map.Entry<String, Integer> el : ls.entrySet()) {
				if (el.getValue() > highest) {
					bests.clear();
					bests.add(el);
					highest = el.getValue();
				} else if (el.getValue() == highest) {
					bests.add(el);
				}
			}
			for (Map.Entry<String, Integer> e : bests) {
				ls.remove(e.getKey());
				int percent = 100 * highest / Math.max(total, 1);
				buff.append("Rank: ").append(i + 1).append(", Self: ").append(percent).append("%, Trace: ")
						.append(LINE_SEPARATOR).append(e.getKey()).append(LINE_SEPARATOR);
				n++;
			}
		}
		return buff.toString();
	}

	/**
	 * Stop and obtain the self execution time of methods, each as a row in CSV.
	 */
	private class CountMap<K> extends HashMap<K, Integer> {
		private static final long serialVersionUID = 1L;

		int limit;
		int ignoreThreshold = 0;

		CountMap(int limit) {
			super(2 * limit);
			this.limit = limit;
		}

		CountMap(CountMap<K> map) {
			super(map);
			this.limit = map.limit;
		}

		void increment(K key) {
			Integer c = get(key);
			put(key, (c == null ? 1 : c + 1));
			while (size() > limit / 2) {
				ignoreThreshold++;
				Iterator<Map.Entry<K, Integer>> ei = entrySet().iterator();
				while (ei.hasNext()) {
					Integer ec = ei.next().getValue();
					if (ec <= ignoreThreshold)
						ei.remove();
				}
			}
		}
	}

}
