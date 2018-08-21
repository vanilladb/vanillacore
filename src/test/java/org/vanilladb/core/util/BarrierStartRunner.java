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
package org.vanilladb.core.util;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public abstract class BarrierStartRunner extends Thread {

	private CyclicBarrier startBarrier;
	private CyclicBarrier endBarrier;

	private Exception exception;

	public BarrierStartRunner(CyclicBarrier startBarrier, CyclicBarrier endBarrier) {
		this.startBarrier = startBarrier;
		this.endBarrier = endBarrier;
	}

	public abstract void runTask();

	public void beforeTask() {

	}

	public void afterTask() {

	}

	public Exception getException() {
		return exception;
	}

	@Override
	public void run() {
		try {
			beforeTask();
		} catch (Exception e) {
			exception = e;
		}
		
		try {
			startBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e1) {
			e1.printStackTrace();
		}
		
		try {
			runTask();
		} catch (Exception e) {
			exception = e;
		}
		
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
		
		try {
			afterTask();
		} catch (Exception e) {
			exception = e;
		}
	}
}
