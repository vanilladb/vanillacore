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
package org.vanilladb.core.remote.storedprocedure;

import java.io.Serializable;

import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;

public class SpResultSet implements Serializable {

	private static final long serialVersionUID = -8409489171990111489L;
	private Record[] records;
	private Schema schema;

	public SpResultSet(Schema schema, Record... records) {
		this.records = records;
		this.schema = schema;
	}

	public Record[] getRecords() {
		return records;
	}

	public Schema getSchema() {
		return schema;
	}
}
