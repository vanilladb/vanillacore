package org.vanilladb.core.util;

import java.lang.management.ManagementFactory;

public class ThreadMXBean {
	
	private static java.lang.management.ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
	
	public static long getCpuTime() {
		return threadBean.getCurrentThreadCpuTime();
	}
	
	public static long getThreadCount() {
		return threadBean.getThreadCount();
	}
}
