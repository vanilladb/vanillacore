/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.vanilladb.core.remote.storedprocedure;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;

public class SpResultSetTest {
	private static Logger logger = Logger.getLogger(SpResultSetTest.class
			.getName());

	@BeforeClass
	public static void init() {
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN SP RESULT SET TEST");
	}

	@Before
	public void setup() {

	}

	@Test
	public void testInsertData() {
		try {
			Schema schema = new Schema();
			schema.addField("status", Type.VARCHAR(10));
			schema.addField("totalAoumnt", Type.DOUBLE);
			schema.addField("cid", Type.INTEGER);
			schema.addField("date", Type.BIGINT);

			Record[] recs = new Record[10];
			for (int i = 0; i < recs.length; i++) {
				SpResultRecord r = new SpResultRecord();
				r.setVal("status", new VarcharConstant("commit"));
				r.setVal("totaFlAoumnt", new DoubleConstant(5962348.1));
				r.setVal("cid", new IntegerConstant(i));
				r.setVal("date", new BigIntConstant(i * 698));
				recs[i] = r;
			}
			SpResultSet sp = new SpResultSet(schema, recs);

			FileOutputStream fos;

			fos = new FileOutputStream("testfile");

			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(sp);
			oos.flush();
			oos.close();

			SpResultSet sp2;
			FileInputStream fis = new FileInputStream("testfile");
			ObjectInputStream ois = new ObjectInputStream(fis);
			sp2 = (SpResultSet) ois.readObject();
			ois.close();

			assertEquals("*****SpResultSetTest: bad serialization",
					sp.getSchema(), sp2.getSchema());

			Record[] rr2 = sp2.getRecords();
			Record[] rr1 = sp.getRecords();
			for (int i = 0; i < rr1.length; i++) {
				assertEquals("*****SpResultSetTest: bad serialization", rr1[i],
						rr2[i]);

			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}
}
