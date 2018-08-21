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
package org.vanilladb.core.query.algebra.multibuffer;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.query.algebra.AbstractJoinPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.materialize.TempTable;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * Non-recursive implementation of the hashjoin algorithm that performs hashing
 * during the preprocessing stage and merging during the scanning stage.
 */
public class HashJoinPlan extends AbstractJoinPlan {
	private Plan lhs, rhs;
	private String fldName1, fldName2;
	private Transaction tx;
	private Schema schema;
	private Histogram hist;

	public HashJoinPlan(Plan lhs, Plan rhs, String fldName1, String fldName2,
			Transaction tx) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.fldName1 = fldName1;
		this.fldName2 = fldName2;
		this.tx = tx;
		schema = new Schema();
		schema.addAll(lhs.schema());
		schema.addAll(rhs.schema());
		hist = joinHistogram(lhs.histogram(), rhs.histogram(), fldName1,
				fldName2);
	}

	@Override
	public Scan open() {
		// create the to-do and ok lists
		List<TempTable> todoV = new ArrayList<TempTable>();
		List<TempTable> todoW = new ArrayList<TempTable>();
		List<TempTable> okV = new ArrayList<TempTable>();
		List<TempTable> okW = new ArrayList<TempTable>();

		// Put [V,W] into the to-do lists.
		todoV.add(copyRecordsFrom(lhs));
		todoW.add(copyRecordsFrom(rhs));

		int iteration = 0;
		while (!todoV.isEmpty()) {
			TempTable ttV = todoV.remove(0);
			TempTable ttW = todoW.remove(0);
			long size = ttW.getTableInfo().open(tx, true).fileSize();
			int avail = tx.bufferMgr().available();
			if (avail >= size) {
				okV.add(ttV);
				okW.add(ttW);
			} else {
				int k = BufferNeeds.bestRoot(size,tx);
				todoV.addAll(partition(ttV, fldName1, k, iteration));
				todoW.addAll(partition(ttW, fldName2, k, iteration));
			}
		}
		return new HashJoinScan(okV, okW, fldName1, fldName2, tx);
	}

	/**
	 * Returns the number of block acceses required to hashjoin the tables. It
	 * does <em>not</em> include the one-time cost of materializing and hashing
	 * the records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return lhs.blocksAccessed() + rhs.blocksAccessed();
	}

	/**
	 * Returns the schema of the join, which is the union of the schemas of the
	 * underlying queries.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return schema;
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return hist;
	}

	@Override
	public long recordsOutput() {
		return (long) hist.recordsOutput();
	}

	@Override
	public String toString() {
		String c2 = rhs.toString();
		String[] cs2 = c2.split("\n");
		String c1 = lhs.toString();
		String[] cs1 = c1.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->HashJoinPlan (#blks=" + blocksAccessed() + ", #recs="
				+ recordsOutput() + ")\n");
		// right child
		for (String child : cs2)
			sb.append("\t").append(child).append("\n");
		;
		// left child
		for (String child : cs1)
			sb.append("\t").append(child).append("\n");
		;
		return sb.toString();
	}

	private TempTable copyRecordsFrom(Plan p) {
		Scan src = p.open();
		Schema sch = p.schema();
		TempTable tt = new TempTable(sch, tx);
		UpdateScan dest = (UpdateScan) tt.open();
		while (src.next()) {
			dest.insert();
			for (String fldname : sch.fields())
				dest.setVal(fldname, src.getVal(fldname));
		}
		src.close();
		dest.close();
		return tt;
	}

	private List<TempTable> partition(TempTable tt, String fldname, int k,
			int iteration) {
		List<TempTable> tables = new ArrayList<TempTable>();
		List<Scan> buckets = new ArrayList<Scan>();
		Schema sch = tt.getTableInfo().schema();
		for (int i = 0; i < k; i++) {
			TempTable t = new TempTable(sch, tx);
			tables.add(t);
			buckets.add(t.open());
		}
		Scan src = tt.open();
		while (src.next()) {
			Constant val = src.getVal(fldname);
			int bkt = hash(val, k, iteration);
			copyRecord(src, (UpdateScan) buckets.get(bkt), sch);
		}
		for (Scan s : buckets)
			s.close();
		return tables;
	}

	private static int hash(Constant val, int k, int n) {
		/*
		 * First hash based on some changing radix, r, then "fold" the hashed
		 * values to k buckets. Since the radix r must not be a multiple of any
		 * previous radix, we use prime here. The constant 100 ensures that a
		 * bucket has no more than 1% amount of values than others due to
		 * folding.
		 */
		int r = 100 * k;
		for (int i = 0; i < n; i++) {
			r = nextPrime(r);
		}
		return (val.hashCode() % r) % k;
	}

	private static int nextPrime(int n) {
		int p = n + 1;
		while (!isPrime(p))
			p++;
		return p;
	}

	private static boolean isPrime(int n) {
		int limit = (int) Math.sqrt(n);
		for (int i = 2; i <= limit; i++) {
			if (n % i == 0)
				return false;
		}
		return true;
	}

	private void copyRecord(Scan src, UpdateScan dest, Schema sch) {
		while (src.next()) {
			dest.insert();
			for (String fldname : sch.fields())
				dest.setVal(fldname, src.getVal(fldname));
		}
	}
}
