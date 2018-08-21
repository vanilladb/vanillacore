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

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.Page;

/**
 * A log record located at a specified position of a specified page. The method
 * {@link #nextVal} reads the values sequentially but has no idea what values
 * are. Thus the client is responsible for knowing how many values are in the
 * log record, and what their types are.
 */
public class BasicLogRecord {
	private LogSeqNum lsn;
	private Page pg;
	private int currentPos;

	/**
	 * A log record located at the specified position of the specified page.
	 * This constructor is called exclusively by {@link LogIterator#next()}.
	 * 
	 * @param pg
	 *            the page containing the log record
	 * @param lsn
	 *            the sequence number of the log record
	 */
	public BasicLogRecord(Page pg, LogSeqNum lsn) {
		this.pg = pg;
		this.lsn = lsn;
		this.currentPos = (int) lsn.offset();
	}

	/**
	 * Returns the next value of this log record.
	 * 
	 * @return the next value
	 */

	/**
	 * Returns the next value of this log record.
	 * 
	 * @param type
	 *            the expected type of the value
	 * @return the next value
	 */
	public Constant nextVal(Type type) {
		Constant val = pg.getVal(currentPos, type);
		currentPos += Page.size(val);
		return val;
	}

	/**
	 * Returns the log sequence number of this log record.
	 * 
	 * @return the LSN
	 */
	public LogSeqNum getLSN() {
		return lsn;
	}
}
