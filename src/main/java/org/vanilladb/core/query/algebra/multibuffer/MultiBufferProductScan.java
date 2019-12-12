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

import org.vanilladb.core.query.algebra.ProductScan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The Scan class for the muti-buffer version of the <em>product</em> operator.
 */
public class MultiBufferProductScan implements Scan {
	private Scan lhsScan, rhsScan = null, prodScan;
	private TableInfo rhsTi;
	private Transaction tx;
	private int rhsChunkSize;
	private long nextBlkNum, rhsFileSize;

	/**
	 * Creates the scan class for the product of the LHS scan and a table.
	 * 
	 * @param lhsScan
	 *            the LHS scan
	 * @param rhsTi
	 *            the metadata for the RHS table
	 * @param tx
	 *            the current transaction
	 */
	public MultiBufferProductScan(Scan lhsScan, TableInfo rhsTi, Transaction tx) {
		this.lhsScan = lhsScan;
		this.rhsTi = rhsTi;
		this.tx = tx;

		rhsFileSize = rhsTi.open(tx, true).fileSize();
		rhsChunkSize = BufferNeeds.bestFactor(rhsFileSize, tx);
	}

	/**
	 * Positions the scan before the first record. That is, the LHS scan is
	 * positioned at its first record, and the RHS scan is positioned before the
	 * first record of the first chunk.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		nextBlkNum = 0;
		useNextChunk();
	}

	/**
	 * Moves to the next record in the current scan. If there are no more
	 * records in the current chunk, then move to the next LHS record and the
	 * beginning of that chunk. If there are no more LHS records, then move to
	 * the next chunk and begin again.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		if (prodScan == null)
			return false;
		while (!prodScan.next())
			if (!useNextChunk())
				return false;
		return true;
	}

	/**
	 * Closes the current scans.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		if (prodScan != null)
			prodScan.close();
	}

	/**
	 * Returns the value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldname) {
		return prodScan.getVal(fldname);
	}

	/**
	 * Returns true if the specified field is in either of the underlying scans.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return prodScan.hasField(fldName);
	}

	private boolean useNextChunk() {
		if (rhsScan != null)
			rhsScan.close();
		if (nextBlkNum >= rhsFileSize)
			return false;
		long end = nextBlkNum + rhsChunkSize - 1;
		if (end >= rhsFileSize)
			end = rhsFileSize - 1;
		rhsScan = new ChunkScan(rhsTi, nextBlkNum, end, tx);
		prodScan = new ProductScan(lhsScan, rhsScan);
		prodScan.beforeFirst();
		nextBlkNum = end + 1;
		return true;
	}
}
