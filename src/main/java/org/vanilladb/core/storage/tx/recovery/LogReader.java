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
package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_CHECKPOINT;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_COMMIT;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_INDEX_FILE_DELETE_END;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_INDEX_FILE_INSERT_END;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_INDEX_PAGE_DELETE;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_INDEX_PAGE_DELETE_CLR;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_INDEX_PAGE_INSERT;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_INDEX_PAGE_INSERT_CLR;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_LOGICAL_ABORT;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_LOGICAL_START;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_RECORD_FILE_DELETE_END;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_RECORD_FILE_INSERT_END;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_ROLLBACK;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_SET_VALUE;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_SET_VALUE_CLR;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_START;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;

public class LogReader {
	private static final int LAST_POS_POINTER = 0;

	// Page
	private int pointerSize = Page.maxSize(Type.INTEGER);
	private Page page = new Page();
	private BlockId currentBlk;
	private int currentPos;

	// Log File
	private long fileSize; // number of blocks

	// Log Record
	private LogRecord currentRec;

	public LogReader(String logFileName) {
		fileSize = VanillaDb.fileMgr().size(logFileName);
		currentBlk = new BlockId(logFileName, 0);
		currentPos = pointerSize * 2; // point to first record
		page.read(currentBlk);
	}

	public boolean nextRecord() {
		// check if there is record at current position
		if (getLastRecordPosition() == currentPos - pointerSize * 2) {
			// check if there is the next block
			if (hasNextBlock()) {
				moveToNextBlock();

				if (hasRecordInCurrentBlock())
					return nextRecord();
				else
					return false;
			} else
				return false;
		}

		// get record
		// TODO : Need to check currentPos type
		currentRec = readRecord(new BasicLogRecord(page, new LogSeqNum(currentBlk.number(), currentPos)));
		// move to next record position
		int nextPos = (Integer) page.getVal(currentPos - pointerSize, Type.INTEGER).asJavaVal();
		currentPos = nextPos + pointerSize;

		return true;
	}

	public String getLogString() {
//		System.out.println(currentRec.getLSN() + currentRec.toString());
		return currentRec.getLSN() + currentRec.toString();
	}

	private LogRecord readRecord(BasicLogRecord rec) {
		int op = (Integer) rec.nextVal(INTEGER).asJavaVal();
		switch (op) {
		case OP_CHECKPOINT:
			return new CheckpointRecord(rec);
		case OP_START:
			return new StartRecord(rec);
		case OP_COMMIT:
			return new CommitRecord(rec);
		case OP_ROLLBACK:
			return new RollbackRecord(rec);
		case OP_SET_VALUE:
			return new SetValueRecord(rec);
		case OP_LOGICAL_START:
			return new LogicalStartRecord(rec);
		case OP_LOGICAL_ABORT:
			return new LogicalAbortRecord(rec);
		case OP_RECORD_FILE_INSERT_END:
			return new RecordFileInsertEndRecord(rec);
		case OP_RECORD_FILE_DELETE_END:
			return new RecordFileDeleteEndRecord(rec);
		case OP_INDEX_FILE_INSERT_END:
			return new IndexInsertEndRecord(rec);
		case OP_INDEX_FILE_DELETE_END:
			return new IndexDeleteEndRecord(rec);
		case OP_INDEX_PAGE_INSERT:
			return new IndexPageInsertRecord(rec);
		case OP_INDEX_PAGE_DELETE:
			return new IndexPageDeleteRecord(rec);
		case OP_SET_VALUE_CLR:
			return new SetValueClr(rec);
		case OP_INDEX_PAGE_INSERT_CLR:
			return new IndexPageInsertClr(rec);
		case OP_INDEX_PAGE_DELETE_CLR:
			return new IndexPageDeleteClr(rec);
		default:
			return null;
		}
	}

	private void moveToNextBlock() {
		BlockId nextBlk = new BlockId(currentBlk.fileName(), currentBlk.number() + 1);
		page.read(nextBlk);
		currentBlk = nextBlk;
		currentPos = pointerSize * 2; // point to first record
	}

	private boolean hasNextBlock() {
		if (currentBlk.number() < fileSize - 1)
			return true;
		return false;
	}

	private boolean hasRecordInCurrentBlock() {
		return getLastRecordPosition() != 0;
	}

	private int getLastRecordPosition() {
		return (Integer) page.getVal(LAST_POS_POINTER, Type.INTEGER).asJavaVal();
	}
}
