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

import static org.vanilladb.core.sql.Type.BIGINT;

import java.util.List;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

public class IndexPageDeleteClr extends IndexPageDeleteRecord implements CompesationLogRecord {
	private LogSeqNum undoNextLSN;

	public IndexPageDeleteClr(long compTxNum, BlockId indexBlkId, boolean isDirPage,
			SearchKeyType keyType, int slotId, LogSeqNum undoNextLSN) {
		super(compTxNum, indexBlkId, isDirPage, keyType, slotId);
		this.undoNextLSN = undoNextLSN;

	}

	public IndexPageDeleteClr(BasicLogRecord rec) {
		super(rec);
		undoNextLSN = new LogSeqNum((Long) rec.nextVal(BIGINT).asJavaVal(), (Long) rec.nextVal(BIGINT).asJavaVal());
	}

	@Override
	public int op() {
		return OP_INDEX_PAGE_DELETE_CLR;
	}

	/**
	 * Does nothing, because compensation log record is redo-Only
	 */
	@Override
	public void undo(Transaction tx) {
		// do nothing

	}

	@Override
	public LogSeqNum getUndoNextLSN() {
		return undoNextLSN;
	}

	@Override
	public String toString() {
		String str = super.toString();
		return str.substring(0, str.length() - 1) + " " + undoNextLSN + ">";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = super.buildRecord();
		rec.set(0, new IntegerConstant(op()));
		rec.add(new BigIntConstant(undoNextLSN.blkNum()));
		rec.add(new BigIntConstant(undoNextLSN.offset()));
		return rec;
	}

}
