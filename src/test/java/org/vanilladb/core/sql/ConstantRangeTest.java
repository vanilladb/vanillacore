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
package org.vanilladb.core.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConstantRangeTest {
	private static Logger logger = Logger.getLogger(ConstantRangeTest.class
			.getName());
	private static final double NINF = Double.NEGATIVE_INFINITY;
	private static final double INF = Double.POSITIVE_INFINITY;
	private static final double NAN = Double.NaN;

	@BeforeClass
	public static void init() {
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN CONSTANT RANGE TEST");
	}

	@Before
	public void setup() {

	}

	public static ConstantRange constantRange(Double low, boolean lowIncl,
			Double high, boolean highIncl) {
		Constant l = low == null ? null : new DoubleConstant(low);
		Constant h = high == null ? null : new DoubleConstant(high);
		return ConstantRange.newInstance(l, lowIncl, h, highIncl);
	}

	public static ConstantRange constantRange(String low, boolean lowIncl,
			String high, boolean highIncl) {
		Constant l = low == null ? null : new VarcharConstant(low);
		Constant h = high == null ? null : new VarcharConstant(high);

		return ConstantRange.newInstance(l, lowIncl, h, highIncl);
	}

	public static void equals(String msg, ConstantRange range, double low,
			boolean lowIncl, double high, boolean highIncl) {
		assertEquals(msg, constantRange(low, lowIncl, high, highIncl)
				.toString(), range.toString());
	}

	@Test
	public void testIsValid() {
		assertTrue("*****ConstantRangeTest: bad validity",
				constantRange(NINF, false, 10d, true).isValid());
		assertTrue("*****ConstantRangeTest: bad validity",
				constantRange(NINF, false, INF, false).isValid());
		assertFalse("*****ConstantRangeTest: bad validity",
				constantRange(NINF, false, NINF, false).isValid());
		assertFalse("*****ConstantRangeTest: bad validity",
				constantRange(2d, false, NAN, false).isValid());
	}

	@Test
	public void testConstantOperations() {
		ConstantRange cr1 = constantRange(-1d, true, 100d, false);
		assertTrue("*****ConstantRangeTest: bad containment",
				cr1.contains(new IntegerConstant(-1)));
		assertFalse("*****ConstantRangeTest: bad containment",
				cr1.contains(new IntegerConstant(100)));
		assertFalse("*****ConstantRangeTest: bad containment",
				cr1.contains(new IntegerConstant(-100)));
	}

	@Test
	public void testRangeOperations() {
		ConstantRange cr1 = constantRange(-1d, true, 100d, false);
		ConstantRange cr2 = constantRange(2d, true, 100d, false);
		ConstantRange cr3 = constantRange(-3d, true, -1d, false);
		ConstantRange cr4 = constantRange(NINF, false, INF, false);
		assertTrue("*****ConstantRangeTest: bad overlapping",
				cr1.isOverlapping(cr2));
		assertFalse("*****ConstantRangeTest: bad overlapping",
				cr1.isOverlapping(cr3));
		assertTrue("*****ConstantRangeTest: bad overlapping",
				cr1.isOverlapping(cr4));
	}

	@Test
	public void testVarcharRange() {
		Type varcharType = Type.VARCHAR(30);
		
		ConstantRange cr1 = constantRange("xyz", true, "abc", false);
		ConstantRange cr2 = ConstantRange
				.newInstance(new VarcharConstant("ggg", varcharType));

		assertTrue("*****ConstantRangeTest: bad isValid for varchar range",
				cr2.isValid());
		assertTrue("*****ConstantRangeTest: bad isConstant for varchar range",
				cr2.isConstant());
		assertFalse("*****ConstantRangeTest: bad isValid for varchar range",
				cr1.isValid());

		Constant v1 = new VarcharConstant("ABCDEFGHIIII", varcharType);
		Constant v2 = new VarcharConstant("ABCDEFGHIIIIJJJ", varcharType);
		Constant v3 = new VarcharConstant("BCDEFGHIIIIJJJ", varcharType);
		Constant v4 = new VarcharConstant("ZZ", varcharType);
		ConstantRange cr3 = ConstantRange.newInstance(v1, true, null, false);
		ConstantRange cr4 = ConstantRange.newInstance(v1);
		assertTrue(
				"*****ConstantRangeTest: bad larger than for varchar range",
				cr3.largerThan(new VarcharConstant("AB"))
						&& !cr3.largerThan(v3) && !cr3.largerThan(v1));
		assertTrue(
				"*****ConstantRangeTest: bad less than for varchar range",
				!cr4.lessThan(new VarcharConstant("AB"))
						&& cr4.lessThan(new VarcharConstant("ZZZZKZK", varcharType)));
		assertTrue("*****ConstantRangeTest: bad contains for varchar range",
				cr3.contains(v1) && cr3.contains(v2) && cr3.contains(v3));
		assertTrue("*****ConstantRangeTest: bad contains for varchar range",
				cr4.contains(v1));
		assertFalse("*****ConstantRangeTest: bad contains for varchar range",
				cr4.contains(v2));
		assertFalse("*****ConstantRangeTest: bad contains for varchar range",
				cr4.contains(v3));

		ConstantRange cr5 = ConstantRange.newInstance(new VarcharConstant("", varcharType),
				true, v1, false);
		assertFalse("*****ConstantRangeTest: bad contains for varchar range",
				cr5.contains(v1));
		assertTrue("*****ConstantRangeTest: bad contains for varchar range",
				cr5.contains(new VarcharConstant("ABCDEFGHII", varcharType)));

		// [BCDEFGHIIIIJJJ, ZZ)
		ConstantRange cr6 = ConstantRange.newInstance(v3, true, v4, false);
		assertTrue("*****ConstantRangeTest: bad isValid for varchar range",
				cr6.isValid());
		assertTrue("*****ConstantRangeTest: bad contains for varchar range",
				cr6.contains(v3));
		assertFalse("*****ConstantRangeTest: bad contains for varchar range",
				cr6.contains(v4));
		assertFalse("*****ConstantRangeTest: bad contains for varchar range",
				cr6.contains(new VarcharConstant("ABCD", varcharType)));
		assertTrue("*****ConstantRangeTest: bad contains for varchar range",
				cr6.contains(new VarcharConstant("EFABCD", varcharType)));
		assertTrue("*****ConstantRangeTest: bad contains for varchar range",
				cr6.contains(new VarcharConstant("FABCDJJJJJZZZZZZZZZ", varcharType)));
		assertFalse("*****ConstantRangeTest: bad contains for varchar range",
				cr6.contains(new VarcharConstant("ZZZZZZZZ", varcharType)));
	}
}
