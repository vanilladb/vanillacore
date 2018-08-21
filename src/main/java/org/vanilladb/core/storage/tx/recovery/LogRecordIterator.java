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
import org.vanilladb.core.storage.log.BasicLogRecord;

/**
 * A class that provides the ability to read records from the log in reverse
 * order. Unlike the similar class
 * {@link org.vanilladb.core.storage.log.LogIterator LogIterator}, this class
 * understands the meaning of the log records.
 */
class LogRecordIterator implements ReversibleIterator<LogRecord> {
	private ReversibleIterator<BasicLogRecord> iter = VanillaDb.logMgr().iterator();

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	/**
	 * Constructs a log record from the values in the current basic log record.
	 * The method first reads an integer, which denotes the type of the log
	 * record. Based on that type, the method calls the appropriate LogRecord
	 * constructor to read the remaining values.
	 * 
	 * @return the next log record, or null if no more records
	 */
	
	@Override
	public LogRecord next() {
		BasicLogRecord rec = iter.next();
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
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasPrevious() {
		return iter.hasPrevious();
	}

	@Override
	public LogRecord previous() {
		BasicLogRecord rec = iter.previous();
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
			throw new UnsupportedOperationException();
		}
	}
}
