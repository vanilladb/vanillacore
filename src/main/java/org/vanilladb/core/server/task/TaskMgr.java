/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
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
package org.vanilladb.core.server.task;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import org.vanilladb.core.util.CoreProperties;

/**
 * The task manager of VanillaCore. This manager is responsible for maintaining
 * the thread pool of worker thread.
 */
public class TaskMgr {
	
	public final static int THREAD_POOL_SIZE;

	static {
		THREAD_POOL_SIZE = CoreProperties.getLoader().getPropertyAsInteger(
				TaskMgr.class.getName() + ".THREAD_POOL_SIZE", 150);
	}
	
	private ExecutorService executor;

	/*
	 * Optimization: WorkStealingPool provides better hand-over latency than FixedThreadPool
	 * because it reduces contention for accessing a single global queue
	 * by creating a queue for each thread.
	 * For more information, please refer to the link below
	 * http://www.h-online.com/developer/features/The-fork-join-framework-in-Java-7-1762357.html
	 * Moreover, using WorkStealingPool is not necessary.
	 * WorkStealingPool can be replaced someday if we prefer code readability to performance.
	 */
	public TaskMgr() {
		executor = Executors.newWorkStealingPool(THREAD_POOL_SIZE);
	}

	public void runTask(Task task) {
		executor.execute(task);
	}
	
	public int getActiveCount() {
		return ((ForkJoinPool) executor).getRunningThreadCount();
	}
}
