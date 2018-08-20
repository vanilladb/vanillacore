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
package org.vanilladb.core.query.algebra.multibuffer;

import org.vanilladb.core.storage.tx.Transaction;

/**
 * A class containing static methods, which estimate the optimal number of
 * buffers to allocate for a scan.
 */
public class BufferNeeds {

	/**
	 * This method considers the various roots of the specified output size (in
	 * blocks), and returns the highest root that is less than the number of
	 * available buffers.
	 * 
	 * @param size
	 *            the size of the output file
	 * @param tx
	 *            the tx to execute
	 * @return the highest number less than the number of available buffers,
	 *         that is a root of the plan's output size
	 */
	public static int bestRoot(long size, Transaction tx) {
		int avail = tx.bufferMgr().available();
		if (avail <= 1)
			return 1;
		int k = Integer.MAX_VALUE;
		double i = 1.0;
		while (k > avail) {
			i++;
			k = (int) Math.ceil(Math.pow(size, 1 / i));
		}
		return k;
	}

	/**
	 * This method considers the various factors of the specified output size
	 * (in blocks), and returns the highest factor that is less than the number
	 * of available buffers.
	 * 
	 * @param size
	 *            the size of the output file
	 * @param tx
	 *            the tx to execute
	 * @return the highest number less than the number of available buffers,
	 *         that is a factor of the plan's output size
	 */
	public static int bestFactor(long size, Transaction tx) {
		int avail = tx.bufferMgr().available();
		if (avail <= 1)
			return 1;
		long k = size;
		double i = 1.0;
		while (k > avail) {
			i++;
			k = (int) Math.ceil(size / i);
		}
		return (int) k;
	}
}
