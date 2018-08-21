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
package org.vanilladb.core.storage.log;

import static org.vanilladb.core.sql.Type.INTEGER;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.tx.recovery.ReversibleIterator;

/**
 * A class that provides the ability to move through the records of the log file
 * in reverse order.
 */
public class LogIterator implements ReversibleIterator<BasicLogRecord> {
	// Optimization: store the size of pointer to other log record
	private int pointerSize = Page.maxSize(INTEGER);
	private BlockId blk;
	private Page pg = new Page();
	private int currentRec;
	private BlockId endBlk;
	private boolean isForward = true;

	/**
	 * Creates an iterator for the records in the log file, positioned after the
	 * last log record. This constructor is called exclusively by
	 * {@link LogMgr#iterator()}.
	 * 
	 * @param blk
	 *            the id of the last block of the log file
	 */
	public LogIterator(BlockId blk) {
		this.blk = this.endBlk = blk;
		pg.read(blk);
		currentRec = (Integer) pg.getVal(LogMgr.LAST_POS, INTEGER).asJavaVal();
	}

	/**
	 * Determines if the current log record is the earliest record in the log
	 * file.
	 * 
	 * @return true if there is an earlier record
	 */
	@Override
	public boolean hasNext() {
		if (!isForward) {
			currentRec = currentRec - pointerSize;
			isForward = true;
		}
		return currentRec > 0 || blk.number() > 0;
	}

	/**
	 * Moves to the next log record in reverse order. If the current log record
	 * is the earliest in its block, then the method moves to the next oldest
	 * block, and returns the log record from there.
	 * 
	 * @return the next earliest log record
	 */
	@Override
	public BasicLogRecord next() {
		if (!isForward) {
			currentRec = currentRec - pointerSize;
			isForward = true;
		}
		if (currentRec == 0)
			moveToNextBlock();
		currentRec = (Integer) pg.getVal(currentRec, INTEGER).asJavaVal();
		return new BasicLogRecord(pg, new LogSeqNum(blk.number(), currentRec + pointerSize * 2));
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasPrevious() {
		if (isForward) {
			currentRec = currentRec + pointerSize;
			isForward = false;
		}
		return (blk.number() < endBlk.number())
				|| (currentRec < (Integer) pg.getVal(LogMgr.LAST_POS, INTEGER).asJavaVal()
						&& blk.number() <= endBlk.number());
	}

	@Override
	public BasicLogRecord previous() {
		if (isForward) {
			currentRec = currentRec + pointerSize;
			isForward = false;
		}
		// if the currentRec point back to the front
		if (currentRec > (Integer) pg.getVal(currentRec, INTEGER).asJavaVal())
			moveToPrevBlock();

		BasicLogRecord record = new BasicLogRecord(pg, new LogSeqNum(blk.number(), currentRec + pointerSize));
		currentRec = (Integer) pg.getVal(currentRec, INTEGER).asJavaVal();
		return record;

	}

	/**
	 * Moves to the next log block in reverse order, and positions it after the
	 * last record in that block.
	 */
	private void moveToNextBlock() {
		blk = new BlockId(blk.fileName(), blk.number() - 1);
		pg.read(blk);
		currentRec = (Integer) pg.getVal(LogMgr.LAST_POS, INTEGER).asJavaVal();
	}

	/**
	 * Moves to the previous log block in reverse order, and positions it after
	 * the last record in that block.
	 */
	private void moveToPrevBlock() {
		blk = new BlockId(blk.fileName(), blk.number() + 1);
		pg.read(blk);
		currentRec = 0 + pointerSize;
	}
}
