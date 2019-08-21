/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
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

public class PeriodicalJob extends Thread {

	private final long startTime;
	private final long preiod;
	private final long totalTime;
	private final Runnable job;
	
	public PeriodicalJob(long preiod, long totalTime, Runnable job) {
		this(System.currentTimeMillis(), preiod, totalTime, job);
	}
	
	public PeriodicalJob(long startTime, long preiod, long totalTime, Runnable job) {
		this.startTime = startTime;
		this.preiod = preiod;
		this.totalTime = totalTime;
		this.job = job;
	}
	
	@Override
	public void run() {
		long currentTime = System.currentTimeMillis();
		long lastTime = startTime;
		
		try {
			while (currentTime - startTime < totalTime) {
				if (currentTime - lastTime > preiod) {
					job.run();
					lastTime =  (currentTime / preiod) * preiod;
				}
				
				if (preiod > 20)
					Thread.sleep(preiod / 20);
				else
					Thread.sleep(1);
				
				currentTime = System.currentTimeMillis();
			}
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}
}
