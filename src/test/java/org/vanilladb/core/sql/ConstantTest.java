/*******************************************************************************
 * Copyright 2017 vanilladb.org
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
package org.vanilladb.core.sql;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;

public class ConstantTest {

	@Test
	public void testConstant() {
		IntegerConstant ic1 = new IntegerConstant(5);
		IntegerConstant ic2 = new IntegerConstant(-9);
		IntegerConstant ic3 = new IntegerConstant(55);

		BigIntConstant lc1 = new BigIntConstant(55);
		BigIntConstant lc2 = new BigIntConstant(559);

		VarcharConstant sc1 = new VarcharConstant("aabdcd");
		VarcharConstant sc2 = new VarcharConstant("aabdcd");
		VarcharConstant sc3 = new VarcharConstant("sssaabdcd");

		assertTrue("*****ConstantTest: bad constant equal to", ic1.equals(ic1));
		assertTrue("*****ConstantTest: bad constant equal to", ic3.equals(lc1));
		assertTrue("*****ConstantTest: bad constant equal to", !ic3.equals(ic2));

		assertTrue("*****ConstantTest: bad constant comparision",
				ic1.compareTo(ic1) == 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				ic1.compareTo(ic2) > 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				ic1.compareTo(ic3) < 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				lc2.compareTo(lc1) > 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				ic1.compareTo(lc2) < 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				ic1.compareTo(lc1) < 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				ic3.compareTo(lc1) == 0);

		assertTrue("*****ConstantTest: bad constant comparision",
				sc1.equals(sc2));
		assertTrue("*****ConstantTest: bad constant comparision",
				!sc1.equals(sc3));
		assertTrue("*****ConstantTest: bad constant comparision",
				sc1.compareTo(sc2) == 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				sc1.compareTo(sc3) < 0);
	}
}
