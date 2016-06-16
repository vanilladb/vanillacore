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
package org.vanilladb.core.storage.metadata.statistics;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.vanilladb.core.query.algebra.AbstractJoinPlan;
import org.vanilladb.core.query.algebra.ProductPlan;
import org.vanilladb.core.query.algebra.ProjectPlan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.aggfn.AggregationFn;
import org.vanilladb.core.sql.aggfn.AvgFn;
import org.vanilladb.core.sql.aggfn.CountFn;
import org.vanilladb.core.sql.aggfn.DistinctCountFn;
import org.vanilladb.core.sql.aggfn.MaxFn;
import org.vanilladb.core.sql.aggfn.MinFn;
import org.vanilladb.core.sql.aggfn.SumFn;
import org.vanilladb.core.sql.predicate.ConstantExpression;
import org.vanilladb.core.sql.predicate.Expression;
import org.vanilladb.core.sql.predicate.FieldNameExpression;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term;

public class HistogramTest {

	public static Schema schema12() {
		Schema sch = new Schema();
		sch.addField("f1", Type.INTEGER);
		sch.addField("f2", Type.INTEGER);
		return sch;
	}

	public static Schema schema3() {
		Schema sch = new Schema();
		sch.addField("f3", Type.INTEGER);
		return sch;
	}

	public static Schema schema4() {
		Schema sch = new Schema();
		sch.addField("f4", Type.INTEGER);
		return sch;
	}

	public static Schema schema5() {
		Schema sch = new Schema();
		sch.addField("f5", Type.INTEGER);
		return sch;
	}

	public static Record record12(final int v1, final int v2) {
		return new Record() {
			Map<String, IntegerConstant> map;

			{
				map = new HashMap<String, IntegerConstant>();
				map.put("f1", new IntegerConstant(v1));
				map.put("f2", new IntegerConstant(v2));
			}

			@Override
			public Constant getVal(String fldName) {
				return map.get(fldName);
			}
		};
	}

	public static Record record3(final int v3) {
		return new Record() {
			Map<String, IntegerConstant> map;

			{
				map = new HashMap<String, IntegerConstant>();
				map.put("f3", new IntegerConstant(v3));
			}

			@Override
			public Constant getVal(String fldName) {
				return map.get(fldName);
			}
		};
	}

	public static Record record4(final int v4) {
		return new Record() {
			Map<String, IntegerConstant> map;

			{
				map = new HashMap<String, IntegerConstant>();
				map.put("f4", new IntegerConstant(v4));
			}

			@Override
			public Constant getVal(String fldName) {
				return map.get(fldName);
			}
		};
	}

	public static Record record5(final int v5) {
		return new Record() {
			Map<String, IntegerConstant> map;

			{
				map = new HashMap<String, IntegerConstant>();
				map.put("f5", new IntegerConstant(v5));
			}

			@Override
			public Constant getVal(String fldName) {
				return map.get(fldName);
			}
		};
	}

	/**
	 * Generates records of f1 and f2 with the following join distribution:
	 * 
	 * <pre>
	 *   f2
	 *   ^
	 *   |                      
	 * 50|____|____|_______|60
	 *   |    |    |       |    
	 *   |    |    |       |    
	 *   |    |    |       |    
	 * 25|10__|40__|10_____|__
	 *   |    |    |       |  
	 * 20|10__|15__|5______|__
	 *   |    |    |       |    
	 * 15|10__|65__|75_____|____ > f1
	 *   10   15   20      50
	 * </pre>
	 * 
	 * The marginal distribution for f1 is:
	 * 
	 * <pre>
	 *        |120       
	 *        |    |90      
	 *        |    |       |60    
	 *   |30__|____|_______|____ > f1
	 *   10   15   20      50
	 * </pre>
	 * 
	 * The marginal distribution for f2 is:
	 * 
	 * <pre>
	 *   |150            
	 *   |                     
	 *   |                     
	 *   |         |60     |60    
	 *   |____|30__|_______|____ > f2
	 *   15   20   25      50
	 * </pre>
	 * 
	 * @return a list of records
	 */
	public static List<Record> records12() {
		List<Record> recs = new ArrayList<Record>(300);
		for (int i = 0; i < 10; i++)
			recs.add(record12(10, 15));
		for (int i = 0; i < 10; i++)
			recs.add(record12(10, 20));
		for (int i = 0; i < 10; i++)
			recs.add(record12(10, 25));

		for (int i = 0; i < 65; i++)
			recs.add(record12(15, 15));
		for (int i = 0; i < 15; i++)
			recs.add(record12(15, 20));
		for (int i = 0; i < 40; i++)
			recs.add(record12(15, 25));

		for (int i = 0; i < 75; i++)
			recs.add(record12(20, 15));
		for (int i = 0; i < 5; i++)
			recs.add(record12(20, 20));
		for (int i = 0; i < 10; i++)
			recs.add(record12(20, 25));

		for (int i = 0; i < 60; i++)
			recs.add(record12(50, 50));
		return recs;
	}

	/**
	 * Generates records of f3 with the following distribution:
	 * 
	 * <pre>
	 *                       |40
	 *        |30            |
	 *        |    |20  |20  |  
	 *   |10__|____|____|____|____ > f3
	 *   12   16   20   24   28
	 * </pre>
	 * 
	 * *
	 * 
	 * @return a list of records
	 */
	public static List<Record> records3() {
		List<Record> recs = new ArrayList<Record>(120);
		for (int i = 0; i < 10; i++)
			recs.add(record3(12));
		for (int i = 0; i < 30; i++)
			recs.add(record3(16));
		for (int i = 0; i < 20; i++)
			recs.add(record3(20));
		for (int i = 0; i < 20; i++)
			recs.add(record3(24));
		for (int i = 0; i < 40; i++)
			recs.add(record3(28));
		return recs;
	}

	/**
	 * Generates records of f4 with the following distribution:
	 * 
	 * <pre>
	 * 										  |40
	 * 										  |
	 *                           			  |
	 *                  | | |10		          |
	 *                  | | | 			      |
	 *   |1|1___________|_|_|_________________|______~~__|1___> f4
	 *   1 2             121314					28	   ~~  400
	 * </pre>
	 * 
	 * 
	 * 
	 * @return a list of records
	 */
	public static List<Record> records4() {
		List<Record> recs = new ArrayList<Record>(300);
		for (int i = 0; i < 1; i++)
			recs.add(record4(1));
		for (int i = 0; i < 1; i++)
			recs.add(record4(2));
		for (int i = 0; i < 10; i++)
			recs.add(record4(12));
		for (int i = 0; i < 10; i++)
			recs.add(record4(13));
		for (int i = 0; i < 10; i++)
			recs.add(record4(14));
		for (int i = 0; i < 40; i++)
			recs.add(record4(28));
		for (int i = 0; i < 1; i++)
			recs.add(record4(400));
		return recs;
	}

	/**
	 * Generates records of f5 with the following distribution:
	 * 
	 * <pre>
	 * 
	 * 
	 *   |20      |20  |20  |20  
	 *   |________|____|____|____ > f5
	 *   16       20   22   24
	 * </pre>
	 * 
	 * *
	 * 
	 * @return a list of records
	 */
	public static List<Record> records5() {
		List<Record> recs = new ArrayList<Record>(120);
		for (int i = 0; i < 20; i++)
			recs.add(record5(16));
		for (int i = 0; i < 20; i++)
			recs.add(record5(20));
		for (int i = 0; i < 20; i++)
			recs.add(record5(22));
		for (int i = 0; i < 20; i++)
			recs.add(record5(24));
		return recs;
	}

	public static Histogram areaHistogram12() {
		SampledHistogramBuilder hb = new SampledHistogramBuilder(schema12());
		List<Record> samples = records12();
		for (Record rec : samples)
			hb.sample(rec);
		return hb.newMaxDiffAreaHistogram(3);
	}

	public static Histogram freqHistogram12() {
		SampledHistogramBuilder hb = new SampledHistogramBuilder(schema12());
		List<Record> samples = records12();
		for (Record rec : samples)
			hb.sample(rec);
		return hb.newMaxDiffFreqHistogram(3, 2);
	}

	public static Histogram areaHistogram3() {
		SampledHistogramBuilder hb = new SampledHistogramBuilder(schema3());
		List<Record> samples = records3();
		for (Record rec : samples)
			hb.sample(rec);
		return hb.newMaxDiffAreaHistogram(3);
	}

	public static Histogram freqHistogram3() {
		SampledHistogramBuilder hb = new SampledHistogramBuilder(schema3());
		List<Record> samples = records3();
		for (Record rec : samples)
			hb.sample(rec);
		return hb.newMaxDiffFreqHistogram(3, 3);
	}

	public static Histogram areaHistogram4() {
		SampledHistogramBuilder hb = new SampledHistogramBuilder(schema4());
		List<Record> samples = records4();
		for (Record rec : samples)
			hb.sample(rec);
		return hb.newMaxDiffAreaHistogram(4);
	}

	public static Histogram freqHistogram4() {
		SampledHistogramBuilder hb = new SampledHistogramBuilder(schema4());
		List<Record> samples = records4();
		for (Record rec : samples)
			hb.sample(rec);
		return hb.newMaxDiffFreqHistogram(3, 5);
	}

	public static Histogram areaHistogram5() {
		SampledHistogramBuilder hb = new SampledHistogramBuilder(schema5());
		List<Record> samples = records5();
		for (Record rec : samples)
			hb.sample(rec);
		return hb.newMaxDiffAreaHistogram(1);
	}

	public static boolean equalString(Collection<String> strings, String[] lines) {
		if (strings.size() != lines.length)
			return false;
		for (String line : lines)
			if (!strings.contains(line))
				return false;
		return true;
	}

	public static boolean equalNumRecs(Histogram hist, int numRecs) {
		return hist.recordsOutput() == numRecs;
	}

	public static boolean equalFields(Histogram hist, String[] flds) {
		Set<String> histFldsSet = hist.fields();
		if (histFldsSet.size() != flds.length)
			return false;
		for (String f : flds)
			if (!histFldsSet.contains(f))
				return false;
		return true;
	}

	public static boolean equalBuckets(Histogram hist, String fldName,
			String[] strings) {
		Collection<Bucket> bkts = hist.buckets(fldName);
		if (bkts.size() != strings.length)
			return false;
		List<String> bktStrings = new ArrayList<String>();
		for (Bucket bkt : bkts)
			bktStrings.add(bkt.toString());
		return equalString(bktStrings, strings);
	}

	@Test
	public void testNewMaxDiffFreqHistogram() {
		// test relation 12
		Histogram fHist12 = freqHistogram12();
		assertTrue("*****HistogramTest: bad MaxDiff(V, F) histogram",
				equalNumRecs(fHist12, 300));
		assertTrue("*****HistogramTest: bad MaxDiff(V, F) histogram",
				equalFields(fHist12, new String[] { "f1", "f2" }));
		/**
		 * 3 expected buckets for f1 are:
		 * 
		 * <pre>
		 * 		'	   '
		 *      ' |120 '        
		 *      ' |    '|90      
		 *      ' |    '|       |60    
		 *   |30'_|____'|_______|____ > f1
		 *   10 ' 15   '20      50
		 *      '      '
		 * </pre>
		 * 
		 * Note that the MaxDiff(V, F) will find the cut at where the diffFreq
		 * is maximal. Here we have diff(15,20) = 30 = diff(20,50) resulting in
		 * cutting at the smallest value.
		 */
		assertTrue(
				"*****HistogramTest: bad MaxDiff(V, F) histogram",
				equalBuckets(
						fHist12,
						"f1",
						new String[] {
								"freq: 150.0, valRange: [20.0, 50.0], distVals: 2.0, pcts: {20: 0.50, 50: 1.00}",
								"freq: 30.0, valRange: [10.0, 10.0], distVals: 1.0, pcts: {10: 1.00}",
								"freq: 120.0, valRange: [15.0, 15.0], distVals: 1.0, pcts: {15: 1.00}" }));

		/**
		 * 3 expected buckets for f2 are:
		 * 
		 * <pre>
		 * 		 '	   '
		 *   |150'     '    
		 *   |   '     '         
		 *   |   '     '        
		 *   |   '     '|60     |60    
		 *   |___'_|30_'|_______|____ > f2
		 *   15  ' 20  '25      50
		 *       '     '
		 * </pre>
		 */
		assertTrue(
				"*****HistogramTest: bad MaxDiff(V, F) histogram",
				equalBuckets(
						fHist12,
						"f2",
						new String[] {
								"freq: 120.0, valRange: [25.0, 50.0], distVals: 2.0, pcts: {25: 0.50, 50: 1.00}",
								"freq: 150.0, valRange: [15.0, 15.0], distVals: 1.0, pcts: {15: 1.00}",
								"freq: 30.0, valRange: [20.0, 20.0], distVals: 1.0, pcts: {20: 1.00}" }));

		/**
		 * 3 expected buckets for f4 are:
		 * 
		 * <pre>
		 * 				   					     '|40		 '
		 * 				   					     '|			 '
		 *                          			 '|			 ' 
		 *                 | | |10		         '|			 '
		 *                 | | | 			     '|          '
		 *   |1|1__________|_|_|_________________'|______~~__'|1___> f4
		 *   1 2           121314				 ' 28	 ~~  ' 400
		 * 				   					     '           '
		 * </pre>
		 * 
		 * Value 400 with one record is an outlier. Note that outliers may waste
		 * the buckets.
		 * 
		 * Construct 5 percentiles in each bucket. The first bucket is expected
		 * to see the percentiles as
		 * 
		 * <pre>
		 * 		{1: 0.20, 2: 0.40, 12: 0.60, 13: 0.80, 14: 1.00}.
		 * </pre>
		 * 
		 * 
		 */
		// test relation 4
		Histogram fHist4 = freqHistogram4();
		assertTrue("*****HistogramTest: bad MaxDiff(V, F) histogram",
				equalNumRecs(fHist4, 73));
		assertTrue("*****HistogramTest: bad MaxDiff(V, F) histogram",
				equalFields(fHist4, new String[] { "f4" }));
		assertTrue(
				"*****HistogramTest: bad MaxDiff(V, F) histogram",
				equalBuckets(
						fHist4,
						"f4",
						new String[] {
								"freq: 1.0, valRange: [400.0, 400.0], distVals: 1.0, pcts: {400: 1.00}",
								"freq: 40.0, valRange: [28.0, 28.0], distVals: 1.0, pcts: {28: 1.00}",
								"freq: 32.0, valRange: [1.0, 14.0], distVals: 5.0, pcts: {1: 0.20, 2: 0.40, 12: 0.60, 13: 0.80, 14: 1.00}" }));
	}

	@Test
	public void testNewMaxDiffAreaHistogram() {
		// test relation 12
		Histogram aHist12 = areaHistogram12();
		assertTrue("*****HistogramTest: bad MaxDiff(V, A) histogram",
				equalNumRecs(aHist12, 300));
		assertTrue("*****HistogramTest: bad MaxDiff(V, A) histogram",
				equalFields(aHist12, new String[] { "f1", "f2" }));
		/**
		 * 3 expected buckets for f1 are:
		 * 
		 * <pre>
		 * 		'			   '
		 *      ' |120         '
		 *      ' |    |90     ' 
		 *      ' |    |       '|60    
		 *   |30'_|____|_______'|____ > f1
		 *   10 ' 15   20      '50
		 *      '              '
		 * </pre>
		 */

		assertTrue(
				"*****HistogramTest: bad MaxDiff(V, A) histogram",
				equalBuckets(
						aHist12,
						"f1",
						new String[] {
								"freq: 60.0, valRange: [50.0, 50.0], distVals: 1.0, pcts: null",
								"freq: 210.0, valRange: [15.0, 20.0], distVals: 2.0, pcts: null",
								"freq: 30.0, valRange: [10.0, 10.0], distVals: 1.0, pcts: null" }));

		/**
		 * 3 expected buckets for f2 are:
		 * 
		 * <pre>
		 * 		 '		   '
		 *   |150'         '   
		 *   |   '         '         
		 *   |   '         '        
		 *   |   '      |60'     |60    
		 *   |___'_|30__|__'_____|____ > f2
		 *   15  ' 20   25 '     50
		 *       '         '
		 * </pre>
		 */
		assertTrue(
				"*****HistogramTest: bad MaxDiff(V, A) histogram",
				equalBuckets(
						aHist12,
						"f2",
						new String[] {
								"freq: 60.0, valRange: [50.0, 50.0], distVals: 1.0, pcts: null",
								"freq: 90.0, valRange: [20.0, 25.0], distVals: 2.0, pcts: null",
								"freq: 150.0, valRange: [15.0, 15.0], distVals: 1.0, pcts: null" }));

		// test relation 3
		Histogram aHist3 = areaHistogram3();
		assertTrue("*****HistogramTest: bad MaxDiff(V, A) histogram",
				equalNumRecs(aHist3, 120));
		assertTrue("*****HistogramTest: bad MaxDiff(V, A) histogram",
				equalFields(aHist3, new String[] { "f3" }));
		/**
		 * 3 expected buckets for f3 are:
		 * 
		 * <pre>
		 * 		'				 '
		 *      '                ' |40
		 *      '  |30           ' |
		 *      '  |    |20  |20 ' |  
		 *   |10'__|____|____|___'_|____ > f3
		 *   12 '  16   20   24  ' 28
		 * 		'				 '
		 * </pre>
		 */
		assertTrue(
				"*****HistogramTest: bad MaxDiff(V, A) histogram",
				equalBuckets(
						aHist3,
						"f3",
						new String[] {
								"freq: 40.0, valRange: [28.0, 28.0], distVals: 1.0, pcts: null",
								"freq: 10.0, valRange: [12.0, 12.0], distVals: 1.0, pcts: null",
								"freq: 70.0, valRange: [16.0, 24.0], distVals: 3.0, pcts: null" }));

		// test relation 4
		Histogram aHist4 = areaHistogram4();
		assertTrue("*****HistogramTest: bad MaxDiff(V, A) histogram",
				equalNumRecs(aHist4, 73));
		assertTrue("*****HistogramTest: bad MaxDiff(V, A) histogram",
				equalFields(aHist4, new String[] { "f4" }));
		/**
		 * 4 expected buckets for f4 are:
		 * 
		 * <pre>
		 * 				   '					 '|40		 '
		 * 				   '					 '|			 '
		 *                 '         			 '|			 ' 
		 *                 '| | |10		         '|			 '
		 *                 '| | | 			     '|          '
		 *   |1|1__________'|_|_|________________'|______~~__'|1___> f4
		 *   1 2           ' 121314				 ' 28	 ~~  ' 400
		 * 				   '					 '           '
		 * </pre>
		 * 
		 * Value 400 with one record is an outlier. Note that outliers may waste
		 * the buckets.
		 */
		assertTrue(
				"*****HistogramTest: bad MaxDiff(V, A) histogram",
				equalBuckets(
						aHist4,
						"f4",
						new String[] {
								"freq: 1.0, valRange: [400.0, 400.0], distVals: 1.0, pcts: null",
								"freq: 40.0, valRange: [28.0, 28.0], distVals: 1.0, pcts: null",
								"freq: 30.0, valRange: [12.0, 14.0], distVals: 3.0, pcts: null",
								"freq: 2.0, valRange: [1.0, 2.0], distVals: 2.0, pcts: null" }));

	}

	@Test
	public void testRangeHistogram() {
		// test relation 3 with range ( 16 <= f3 <= 26)
		Histogram aHist3 = areaHistogram3();
		ConstantRange searchRange = ConstantRange.newInstance(
				new DoubleConstant(16), true, new DoubleConstant(26), true);
		Map<String, ConstantRange> rangeMap = new HashMap<String, ConstantRange>();
		rangeMap.put("f3", searchRange);
		Histogram rangeHist = SelectPlan.constantRangeHistogram(aHist3,
				rangeMap);
		/**
		 * 
		 * <pre>
		 * 		'				 '
		 *      '                ' |40
		 *      '  |30           ' |
		 *      '  |    |20  |20 ' |  
		 *   |10'__|____|____|___'_|____ > f3
		 *   12 '  16   20   24  ' 28
		 * 		'				 '
		 * </pre>
		 */
		assertTrue(
				"*****HistogramTest: bad range histogram (16<=f3<=26)",
				equalBuckets(
						rangeHist,
						"f3",
						new String[] { "freq: 70.0, valRange: [16.0, 24.0], distVals: 3.0, pcts: null" }));

		// test relation 4 with range ( 12 <= f4 <= 14)
		Histogram fHist4 = freqHistogram4();
		/**
		 * 3 expected buckets for f4 are:
		 * 
		 * <pre>
		 * 				   					     '|40		 '
		 * 				   					     '|			 '
		 *                          			 '|			 ' 
		 *                 | | |10		         '|			 '
		 *                 | | | 			     '|          '
		 *   |1|1__________|_|_|_________________'|______~~__'|1___> f4
		 *   1 2           121314				 ' 28	 ~~  ' 400
		 * 				   					     '           '
		 * </pre>
		 */
		searchRange = ConstantRange.newInstance(new DoubleConstant(16), true,
				new DoubleConstant(26), true);
		rangeMap.clear();
		rangeMap.put("f4", searchRange);
		rangeHist = SelectPlan.constantRangeHistogram(fHist4, rangeMap);
		assertTrue("*****HistogramTest: bad range histogram (16<=f4<=26)",
				rangeHist.buckets("f4").size() == 0);

		// test relation 4 with range ( 1 <= f4 <= 10)
		searchRange = ConstantRange.newInstance(new DoubleConstant(1), true,
				new DoubleConstant(10), true);
		rangeMap.clear();
		rangeMap.put("f4", searchRange);
		rangeHist = SelectPlan.constantRangeHistogram(fHist4, rangeMap);
		// expected freq = 35*(2/5) = 12.8
		assertTrue(
				"*****HistogramTest: bad range histogram (1<=f4<=10)",
				equalBuckets(
						rangeHist,
						"f4",
						new String[] { "freq: 12.8, valRange: [1.0, 10.0], distVals: 2.0, pcts: {1: 0.50, 2: 1.00}" }));

		// test relation 4 with range ( 12 < f4 < 30)
		searchRange = ConstantRange.newInstance(new DoubleConstant(12), false,
				new DoubleConstant(30), false);
		rangeMap.clear();
		rangeMap.put("f4", searchRange);
		rangeHist = SelectPlan.constantRangeHistogram(fHist4, rangeMap);

		assertTrue(
				"*****HistogramTest: bad range histogram (12<f4<30)",
				equalBuckets(
						rangeHist,
						"f4",
						new String[] {
								"freq: 40.0, valRange: [28.0, 28.0], distVals: 1.0, pcts: {28: 1.00}",
								"freq: 12.8, valRange: (12.0, 14.0], distVals: 2.0, pcts: {13: 0.50, 14: 1.00}" }));

	}

	@Test
	public void testPredHistogram() {
		// test relation 12
		Histogram aHist12 = areaHistogram12();
		/**
		 * 3 expected buckets for f1 are:
		 * 
		 * <pre>
		 * 		'			   '
		 *      ' |120         '
		 *      ' |    |90     ' 
		 *      ' |    |       '|60    
		 *   |30'_|____|_______'|____ > f1
		 *   10 ' 15   20      '50
		 *      '              '
		 * 
		 * 3 expected buckets for f2 are:
		 * 
		 * 		 '		   '
		 *   |150'         '   
		 *   |   '         '         
		 *   |   '         '        
		 *   |   '      |60'     |60    
		 *   |___'_|30__|__'_____|____ > f2
		 *   15  ' 20   25 '     50
		 *       '         '
		 * </pre>
		 */
		// f1 < 20
		Expression e1 = new FieldNameExpression("f1");
		Expression e2 = new ConstantExpression(new IntegerConstant(20));
		Term t1 = new Term(e1, Term.OP_LT, e2);
		Predicate p1 = new Predicate(t1);

		Histogram predHist = SelectPlan.predHistogram(aHist12, p1);
		assertTrue(
				"*****HistogramTest: bad predicate histogram (f1<10)",
				equalBuckets(
						predHist,
						"f1",
						new String[] {
								"freq: 105.0, valRange: [15.0, 20.0), distVals: 1.0, pcts: null",
								"freq: 30.0, valRange: [10.0, 10.0], distVals: 1.0, pcts: null" }));
		/*
		 * Because f1's records reduce to 135, the freqs in f2 should also
		 * reduce to 45% (135/300)
		 */
		assertTrue(
				"*****HistogramTest: bad predicate histogram (f1<10)",
				equalBuckets(
						predHist,
						"f2",
						new String[] {
								"freq: 27.0, valRange: [50.0, 50.0], distVals: 1.0, pcts: null",
								"freq: 40.5, valRange: [20.0, 25.0], distVals: 2.0, pcts: null",
								"freq: 67.5, valRange: [15.0, 15.0], distVals: 1.0, pcts: null" }));

		// f1 < 10 and f1 = f2
		Expression e3 = new FieldNameExpression("f1");
		Expression e4 = new FieldNameExpression("f2");
		Term t2 = new Term(e3, Term.OP_EQ, e4);
		p1.conjunctWith(t2);
		predHist = SelectPlan.predHistogram(aHist12, p1);
		// the expected freq is (105/1) * (67.15/135) = 52.5
		assertTrue(
				"*****HistogramTest: bad predicate histogram (f1<10 and f1=f2)",
				equalBuckets(
						predHist,
						"f1",
						new String[] { "freq: 52.5, valRange: [15.0, 15.0], distVals: 1.0, pcts: null" }));
		assertTrue(
				"*****HistogramTest: bad predicate histogram (f1<10 and f1=f2)",
				equalBuckets(
						predHist,
						"f2",
						new String[] { "freq: 52.5, valRange: [15.0, 15.0], distVals: 1.0, pcts: null" }));

	}

	@Test
	public void testProductHistogram() {
		// test relation 12 product on relation 3
		Histogram aHist12 = areaHistogram12();
		Histogram aHist3 = areaHistogram3();
		Histogram prodHist = ProductPlan.productHistogram(aHist3, aHist12);
		// numRecs = 300 * 120
		assertTrue("*****HistogramTest: bad product histogram",
				equalNumRecs(prodHist, 36000));
		assertTrue("*****HistogramTest: bad product histogram",
				equalFields(prodHist, new String[] { "f1", "f2", "f3" }));
		assertTrue(
				"*****HistogramTest: bad product histogram",
				equalBuckets(
						prodHist,
						"f1",
						new String[] {
								"freq: 7200.0, valRange: [50.0, 50.0], distVals: 1.0, pcts: null",
								"freq: 25200.0, valRange: [15.0, 20.0], distVals: 2.0, pcts: null",
								"freq: 3600.0, valRange: [10.0, 10.0], distVals: 1.0, pcts: null" }));
		assertTrue(
				"*****HistogramTest: bad product histogram",
				equalBuckets(
						prodHist,
						"f2",
						new String[] {
								"freq: 7200.0, valRange: [50.0, 50.0], distVals: 1.0, pcts: null",
								"freq: 10800.0, valRange: [20.0, 25.0], distVals: 2.0, pcts: null",
								"freq: 18000.0, valRange: [15.0, 15.0], distVals: 1.0, pcts: null" }));
		assertTrue(
				"*****HistogramTest: bad product histogram",
				equalBuckets(
						prodHist,
						"f3",
						new String[] {
								"freq: 12000.0, valRange: [28.0, 28.0], distVals: 1.0, pcts: null",
								"freq: 3000.0, valRange: [12.0, 12.0], distVals: 1.0, pcts: null",
								"freq: 21000.0, valRange: [16.0, 24.0], distVals: 3.0, pcts: null" }));
	}

	@Test
	public void testProjectHistogram() {
		Histogram aHist12 = areaHistogram12();
		Histogram projHist = ProjectPlan.projectHistogram(aHist12,
				new HashSet<String>(Arrays.asList("f1")));
		assertTrue("*****HistogramTest: bad project histogram",
				equalNumRecs(projHist, 300));
		assertTrue("*****HistogramTest: bad project histogram",
				equalFields(projHist, new String[] { "f1" }));
		assertTrue(
				"*****HistogramTest: bad project histogram",
				equalBuckets(
						projHist,
						"f1",
						new String[] {
								"freq: 60.0, valRange: [50.0, 50.0], distVals: 1.0, pcts: null",
								"freq: 210.0, valRange: [15.0, 20.0], distVals: 2.0, pcts: null",
								"freq: 30.0, valRange: [10.0, 10.0], distVals: 1.0, pcts: null" }));
	}

	@Test
	public void testJoinHistogram() {
		// test relation 12 product on relation 3
		Histogram aHist5 = areaHistogram5();
		Histogram aHist3 = areaHistogram3();
		Histogram joinHist = AbstractJoinPlan.joinHistogram(aHist5, aHist3,
				"f5", "f3");
		// expected NumRecs = [(30+20+20)/3 * (20+20+20+20)/4] * 3 = 1400
		assertTrue("*****HistogramTest: bad project histogram",
				equalNumRecs(joinHist, 1400));
		assertTrue("*****HistogramTest: bad project histogram",
				equalFields(joinHist, new String[] { "f3", "f5" }));
		/**
		 * 
		 * <pre>
		 * 
		 * 		'				 '
		 *      '                ' |40
		 *      '  |30           ' |
		 *      '  |    |20  |20 ' |  
		 *   |10'__|____|____|___'_|____ > f3
		 *   12 '  16   20   24  ' 28
		 * 		'				 '
		 * 
		 * 
		 *   |20      |20  |20  |20  
		 *   |________|____|____|____ > f5
		 *   16       20   22   24
		 * </pre>
		 */

		assertTrue(
				"*****HistogramTest: bad join histogram (f5=f3)",
				equalBuckets(
						joinHist,
						"f3",
						new String[] { "freq: 1400.0, valRange: [16.0, 24.0], distVals: 3.0, pcts: null" }));
		assertTrue(
				"*****HistogramTest: bad join histogram (f5=f3)",
				equalBuckets(
						joinHist,
						"f5",
						new String[] { "freq: 1400.0, valRange: [16.0, 24.0], distVals: 3.0, pcts: null" }));
	}

	@Test
	public void testGroupByHistogram() {
		Histogram aHist12 = areaHistogram12();
		Set<String> gFlds = new HashSet<String>();
		gFlds.add("f1");
		Set<AggregationFn> aggFns = new HashSet<AggregationFn>();
		aggFns.add(new SumFn("f2"));
		aggFns.add(new AvgFn("f2"));
		aggFns.add(new CountFn("f2"));
		aggFns.add(new DistinctCountFn("f2"));
		aggFns.add(new MinFn("f2"));
		aggFns.add(new MaxFn("f2"));
		Histogram groupByHist = GroupByPlan.groupByHistogram(aHist12, gFlds,
				aggFns);
		// expects 4 group
		assertTrue("*****HistogramTest: bad group-by histogram histogram",
				equalNumRecs(groupByHist, 4));
		assertTrue(
				"*****HistogramTest: bad MaxDiff(V, A) histogram",
				equalFields(groupByHist, new String[] { "f1", "avgoff2",
						"countoff2", "dstcountoff2", "maxoff2", "minoff2",
						"sumoff2" }));

		/**
		 * 
		 * <pre>
		 * 		'			   '
		 *      ' |120         '
		 *      ' |    |90     ' 
		 *      ' |    |       '|60    
		 *   |30'_|____|_______'|____ > f1
		 *   10 ' 15   20      '50
		 *      '              '
		 * 
		 * 		 '		   '
		 *   |150'         '   
		 *   |   '         '         
		 *   |   '         '        
		 *   |   '      |60'     |60    
		 *   |___'_|30__|__'_____|____ > f2
		 *   15  ' 20   25 '     50
		 *       '         '
		 * </pre>
		 */
		// avg's bucket range: [15, 50]
		assertTrue(
				"*****HistogramTest: bad group-by histogram groupby(f1) and select avg(f2)",
				equalBuckets(
						groupByHist,
						"avgoff2",
						new String[] { "freq: 4.0, valRange: [15.0, 50.0], distVals: 4.0, pcts: null" }));

		// count's bucket range: [1, 300-4+1]
		assertTrue(
				"*****HistogramTest: bad group-by histogram groupby(f1) and select count(f2)",
				equalBuckets(
						groupByHist,
						"countoff2",
						new String[] { "freq: 4.0, valRange: [1.0, 297.0], distVals: 4.0, pcts: null" }));
		// dstcount's bucket range: [1, 4]
		assertTrue(
				"*****HistogramTest: bad group-by histogram groupby(f1) and select dstcount(f2)",
				equalBuckets(
						groupByHist,
						"dstcountoff2",
						new String[] { "freq: 4.0, valRange: [1.0, 4.0], distVals: 4.0, pcts: null" }));

		// max's bucket range: [15, 50]
		assertTrue(
				"*****HistogramTest: bad group-by histogram groupby(f1) and select max(f2)",
				equalBuckets(
						groupByHist,
						"maxoff2",
						new String[] { "freq: 4.0, valRange: [15.0, 50.0], distVals: 4.0, pcts: null" }));
		// min's bucket range: [15, 50]
		assertTrue(
				"*****HistogramTest: bad group-by histogram groupby(f1) and select min(f2)",
				equalBuckets(
						groupByHist,
						"minoff2",
						new String[] { "freq: 4.0, valRange: [15.0, 50.0], distVals: 4.0, pcts: null" }));
		// sum's bucket range: [15, 7456]
		assertTrue(
				"*****HistogramTest: bad group-by histogram groupby(f1) and select sum(f2)",
				equalBuckets(
						groupByHist,
						"sumoff2",
						new String[] { "freq: 4.0, valRange: [15.0, 7456.0], distVals: 4.0, pcts: null" }));
	}
}
