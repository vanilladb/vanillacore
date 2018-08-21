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

import static org.vanilladb.core.storage.record.FileHeaderPage.NO_SLOT_BLOCKID;
import static org.vanilladb.core.storage.record.FileHeaderPage.NO_SLOT_RID;
import static org.vanilladb.core.storage.record.FileHeaderPage.OFFSET_LDS_BLOCKID;
import static org.vanilladb.core.storage.record.FileHeaderPage.OFFSET_LDS_RID;
import static org.vanilladb.core.storage.record.FileHeaderPage.OFFSET_TS_BLOCKID;
import static org.vanilladb.core.storage.record.FileHeaderPage.OFFSET_TS_RID;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.PageFormatter;

/**
 * An object that can format a page to look like a the header of a file.
 */
public class FileHeaderFormatter extends PageFormatter {

	@Override
	public void format(Buffer buf) {
		// initial the last free slot
		setVal(buf, OFFSET_LDS_BLOCKID, new BigIntConstant(NO_SLOT_BLOCKID));
		setVal(buf, OFFSET_LDS_RID, new IntegerConstant(NO_SLOT_RID));

		// initial the tail slot
		setVal(buf, OFFSET_TS_BLOCKID, new BigIntConstant(NO_SLOT_BLOCKID));
		setVal(buf, OFFSET_TS_RID, new IntegerConstant(NO_SLOT_RID));
	}

}
