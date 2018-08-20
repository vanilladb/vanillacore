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
package org.vanilladb.core.storage.log;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.Page;

public class LogSeqNum implements Comparable<LogSeqNum> {

	public static final int SIZE = Type.BIGINT.maxSize() * 2;
	
	public static final LogSeqNum DEFAULT_VALUE = new LogSeqNum(-1, -1);

	private final long blkNum, offset;
	private final int hashCode;

	public static LogSeqNum readFromPage(Page page, int pos) {
		long blkNum = (long) page.getVal(pos, Type.BIGINT).asJavaVal();
		long offset = (long) page.getVal(pos + Type.BIGINT.maxSize(), Type.BIGINT).asJavaVal();

		return new LogSeqNum(blkNum, offset);
	}

	public LogSeqNum(long blkNum, long offset) {
		this.blkNum = blkNum;
		this.offset = offset;

		// Generate the hash code
		// Don't ask why, just magic.
		int hashCode = 17;
		hashCode = 31 * hashCode + (int) (blkNum ^ (blkNum >>> 32));
		hashCode = 31 * hashCode + (int) (offset ^ (offset >>> 32));
		this.hashCode = hashCode;
	}

	public long blkNum() {
		return blkNum;
	}

	public long offset() {
		return offset;
	}

	// XXX: This might be not needed
	public Constant[] toConstants() {
		return new Constant[] { new BigIntConstant(blkNum), new BigIntConstant(offset) };
	}

	public void writeToPage(Page page, int pos) {
		page.setVal(pos, new BigIntConstant(blkNum));
		page.setVal(pos + Type.BIGINT.maxSize(), new BigIntConstant(offset));
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;

		if (!obj.getClass().equals(LogSeqNum.class))
			return false;

		LogSeqNum lsn = (LogSeqNum) obj;
		return lsn.blkNum == this.blkNum && lsn.offset == this.offset;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public String toString() {
		return "[" + blkNum + ", " + offset + "]";
	}

	@Override
	public int compareTo(LogSeqNum lsn) {
		// Compare the block numbers
		if (blkNum < lsn.blkNum)
			return -1;
		else if (blkNum > lsn.blkNum)
			return 1;

		// Compare the offsets
		if (offset < lsn.offset)
			return -1;
		else if (offset > lsn.offset)
			return 1;

		// All of them are the same
		return 0;
	}
}
