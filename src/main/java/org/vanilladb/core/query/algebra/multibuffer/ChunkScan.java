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

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The class for the <em>chunk</em> operator.
 */
public class ChunkScan implements Scan {
	private List<RecordPage> pages;
	private long startBlkNum, endBlkNum, current;
	private Schema schema;
	private RecordPage rp;
	private String fileName;

	/**
	 * Creates a chunk consisting of the specified pages.
	 * 
	 * @param ti
	 *            the metadata for the chunked table
	 * @param startBlkNum
	 *            the starting block number
	 * @param endBlkNum
	 *            the ending block number
	 * @param tx
	 *            the current transaction
	 */
	public ChunkScan(TableInfo ti, long startBlkNum, long endBlkNum,
			Transaction tx) {
		pages = new ArrayList<RecordPage>();
		this.startBlkNum = startBlkNum;
		this.endBlkNum = endBlkNum;
		this.schema = ti.schema();
		this.fileName = ti.fileName();
		for (long i = startBlkNum; i <= endBlkNum; i++) {
			BlockId blk = new BlockId(fileName, i);
			pages.add(new RecordPage(blk, ti, tx, true));
		}
	}

	/**
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		moveToBlock(startBlkNum);
	}

	/**
	 * Moves to the next record in the current block of the chunk. If there are
	 * no more records, then make the next block be current. If there are no
	 * more blocks in the chunk, return false.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		while (true) {
			if (rp.next())
				return true;
			if (current == endBlkNum)
				return false;
			moveToBlock(current + 1);
		}
	}

	/**
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		for (RecordPage r : pages)
			r.close();
	}

	/**
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		return rp.getVal(fldName);
	}

	/**
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return schema.hasField(fldName);
	}

	private void moveToBlock(long blkNum) {
		current = blkNum;
		rp = pages.get((int) (current - startBlkNum));
		rp.moveToId(-1);
	}
}
