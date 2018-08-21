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
package org.vanilladb.core.storage.index.btree;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.Map;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.PageFormatter;
import org.vanilladb.core.storage.file.Page;

/**
 * Formats a B-tree page.
 * 
 * @see BTreePage
 */
public class BTPageFormatter extends PageFormatter {
	private Schema sch;
	private Map<String, Integer> myOffsetMap;
	private long[] flags;

	/**
	 * Creates a formatter.
	 * 
	 * @param sch
	 *            the schema of the page
	 * @param flags
	 *            the page's flag values
	 */
	public BTPageFormatter(Schema sch, long[] flags) {
		this.sch = sch;
		this.flags = flags;
		myOffsetMap = BTreePage.offsetMap(sch);
	}

	/**
	 * Formats the page by initializing as many index-record slots as possible
	 * to have default values.
	 * 
	 * @see PageFormatter#format(Buffer)
	 */
	@Override
	public void format(Buffer buf) {
		int pos = 0;
		// initial the number of records as 0
		setVal(buf, pos, Constant.defaultInstance(INTEGER));
		int flagSize = Page.maxSize(BIGINT);
		pos += Page.maxSize(INTEGER);
		// set flags
		for (int i = 0; i < flags.length; i++) {
			setVal(buf, pos, new BigIntConstant(flags[i]));
			pos += flagSize;
		}
		int slotSize = BTreePage.slotSize(sch);
		for (int p = pos; p + slotSize <= Buffer.BUFFER_SIZE; p += slotSize)
			makeDefaultRecord(buf, p);
	}

	private void makeDefaultRecord(Buffer buf, int pos) {
		int offset;
		for (String fldname : sch.fields()) {
			offset = myOffsetMap.get(fldname);
			setVal(buf, pos + offset,
					Constant.defaultInstance(sch.type(fldname)));
		}
	}
}
