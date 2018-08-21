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
package org.vanilladb.core.storage.record;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.record.RecordPage.EMPTY;

import java.util.Map;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.PageFormatter;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;

/**
 * An object that can format a page to look like a block of empty records.
 */
public class RecordFormatter extends PageFormatter {
	private TableInfo ti;
	private Map<String, Integer> myOffsetMap;
	// Optimization: store the size of pointer to other log record
	private int flagSize = Page.maxSize(INTEGER);

	/**
	 * Creates a formatter for a new page of a table.
	 * 
	 * @param ti
	 *            the table's metadata
	 */
	public RecordFormatter(TableInfo ti) {
		this.ti = ti;
		myOffsetMap = RecordPage.offsetMap(ti.schema());
	}

	/**
	 * Formats the page by allocating as many record slots as possible, given
	 * the record size. Each record slot is assigned a flag of EMPTY. Each
	 * numeric field is given a value of 0, and each string field is given a
	 * value of "".
	 * 
	 * @see org.vanilladb.core.storage.buffer.PageFormatter#format(org.vanilladb.core.storage.buffer.Buffer)
	 */
	@Override
	public void format(Buffer buf) {
		int slotSize = RecordPage.slotSize(ti.schema());
		Constant emptyFlag = new IntegerConstant(EMPTY);
		for (int pos = 0; pos + slotSize <= Buffer.BUFFER_SIZE; pos += slotSize) {
			setVal(buf, pos, emptyFlag);
			makeDefaultRecord(buf, pos);
		}
	}

	private void makeDefaultRecord(Buffer buf, int pos) {
		int offset;
		for (String fldname : ti.schema().fields()) {
			offset = myOffsetMap.get(fldname);
			setVal(buf, pos + flagSize + offset,
					Constant.defaultInstance(ti.schema().type(fldname)));
		}
	}
}
