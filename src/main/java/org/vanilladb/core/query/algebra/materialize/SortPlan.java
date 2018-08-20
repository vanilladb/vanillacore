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
package org.vanilladb.core.query.algebra.materialize;

import static org.vanilladb.core.sql.RecordComparator.DIR_ASC;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.multibuffer.BufferNeeds;
import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.record.RecordFormatter;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The {@link Plan} class for the <em>sort</em> operator.
 */
public class SortPlan implements Plan {
	private Plan p;
	private RecordComparator comp;
	private Transaction tx;
	private Schema schema;

	private List<String> sortFlds;
	private List<Integer> sortDirs;

	/**
	 * Creates a sort plan for the specified query.
	 * 
	 * @param p
	 *            the plan for the underlying query
	 * @param sortFields
	 *            the fields to sort by
	 * @param sortDirs
	 *            the sort direction
	 * @param tx
	 *            the calling transaction
	 */
	public SortPlan(Plan p, List<String> sortFields, List<Integer> sortDirs,
			Transaction tx) {
		this.p = p;
		comp = new RecordComparator(sortFields, sortDirs);
		this.sortFlds = sortFields;
		this.sortDirs = sortDirs;
		this.tx = tx;
		schema = p.schema();
	}

	public SortPlan(Plan p, List<String> sortFlds, Transaction tx) {
		this.p = p;
		this.sortFlds = sortFlds;
		List<Integer> sortDirs = new ArrayList<Integer>(sortFlds.size());
		for (int i = 0; i < sortFlds.size(); i++)
			sortDirs.add(DIR_ASC);
		this.sortDirs = sortDirs;
		comp = new RecordComparator(sortFlds, sortDirs);
		this.tx = tx;
		schema = p.schema();
	}

	/**
	 * This method is where most of the action is. Up to 2 sorted temporary
	 * tables are created, and are passed into SortScan for final merging.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan src = p.open();
		List<TempTable> runs = splitIntoRuns(src);
		/*
		 * If the input source scan has no record, the temp table list will
		 * result in size 0. Need to check the size of "runs" here.
		 */
		if (runs.size() == 0)
			return src;
		src.close();
		while (runs.size() > 2)
			runs = doAMergeIteration(runs);
		return new SortScan(runs, comp);
	}

	/**
	 * Returns the number of blocks in the sorted table, which is the same as it
	 * would be in a materialized table. It does <em>not</em> include the
	 * one-time cost of materializing and sorting the records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		// does not include the one-time cost of sorting
		Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
		return mp.blocksAccessed();
	}

	/**
	 * Returns the schema of the sorted table, which is the same as in the
	 * underlying query.
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
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		return p.recordsOutput();
	}

	private List<TempTable> splitIntoRuns(Scan src) {
		List<TempTable> temps = new ArrayList<TempTable>();

		src.beforeFirst();
		// if src is empty, return nothing directly
		if (!src.next())
			return temps;

		TempTable currenttemp = new TempTable(schema, tx);
		temps.add(currenttemp);
		TableScan currentscan = (TableScan) currenttemp.open();

		int tblcount = 0;
		long txnum = tx.getTransactionNumber();
		String tblName = "_tempRecordFile";
		TableInfo ti = new TableInfo(tblName + "-" + txnum + "-" + tblcount,
				schema);

		RecordFormatter fmtr = new RecordFormatter(ti);
		Buffer buff = tx.bufferMgr().pinNew(
				tblName + "-" + txnum + "-" + tblcount, fmtr);
		TempRecordPage trp = new TempRecordPage(buff.block(), ti, tx);
		// trp.runAllSlot();

		trp.moveToPageHead();
		int flag;

		while (true) {
			while ((flag = trp.insertFromScan(src)) > 0)
				;
			trp.sortbyselection(this.sortFlds, this.sortDirs);
			trp.moveToPageHead();
			while (trp.copyToScan(currentscan))
				;
			// trp.runAllSlot();
			trp.close();
			if (flag == -1)
				break;

			tblcount++;
			ti = new TableInfo(tblName + "-" + txnum + "-" + tblcount, schema);
			fmtr = new RecordFormatter(ti);
			buff = tx.bufferMgr().pinNew(
					tblName + "-" + txnum + "-" + tblcount, fmtr);
			trp = new TempRecordPage(buff.block(), ti, tx);
			trp.moveToPageHead();

			currentscan.close();
			currenttemp = new TempTable(schema, tx);
			temps.add(currenttemp);
			currentscan = (TableScan) currenttemp.open();
		}
		currentscan.close();
		return temps;
	}

	// private List<TempTable> doAMergeIteration(List<TempTable> runs) {
	// List<TempTable> result = new ArrayList<TempTable>();
	// while (runs.size() > 1) {
	// TempTable p1 = runs.remove(0);
	// TempTable p2 = runs.remove(0);
	// result.add(mergeTwoRuns(p1, p2));
	// }
	// if (runs.size() == 1)
	// result.add(runs.get(0));
	// return result;
	// }

	private List<TempTable> doAMergeIteration(List<TempTable> runs) {
		List<TempTable> result = new ArrayList<TempTable>();
		int numofbuf = BufferNeeds.bestRoot(runs.size(),tx);
		TempTable ps[] = new TempTable[numofbuf];
		while (runs.size() > numofbuf) {
			for (int i = 0; i < numofbuf; i++)
				ps[i] = runs.remove(0);
			result.add(mergeServeralRuns(ps));
		}
		int lastruns = runs.size();
		if (lastruns > 1) {
			TempTable qq[] = new TempTable[lastruns];
			for (int i = 0; i < lastruns; i++)
				qq[i] = runs.remove(0);
			result.add(mergeServeralRuns(qq));
		} else if (lastruns == 1)
			result.add(runs.remove(0));
		return result;
	}

	private TempTable mergeServeralRuns(TempTable... ps) {
		int num = ps.length;
		Scan srcs[] = new Scan[num];
		for (int i = 0; i < num; i++) {
			srcs[i] = ps[i].open();
			srcs[i].beforeFirst();
		}
		TempTable result = new TempTable(schema, tx);
		UpdateScan dest = result.open();
		boolean hasmores[] = new boolean[num];
		int count = 0;
		for (int i = 0; i < num; i++) {
			hasmores[i] = srcs[i].next();
			if (hasmores[i])
				count++;
		}
		while (count > 0) {
			int target = -1;
			for (int i = 0; i < num; i++) {
				if (!hasmores[i])
					continue;
				if (target < 0 || comp.compare(srcs[i], srcs[target]) < 0)
					target = i;
			}
			hasmores[target] = copy(srcs[target], dest);
			if (!hasmores[target])
				count--;
		}
		for (int i = 0; i < num; i++)
			srcs[i].close();
		dest.close();
		return result;
	}
	
	private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
		Scan src1 = p1.open();
		Scan src2 = p2.open();
		TempTable result = new TempTable(schema, tx);
		UpdateScan dest = result.open();
		src1.beforeFirst();
		src2.beforeFirst();
		boolean hasmore1 = src1.next();
		boolean hasmore2 = src2.next();
		while (hasmore1 && hasmore2) {
			if (comp.compare(src1, src2) < 0)
				hasmore1 = copy(src1, dest);
			else
				hasmore2 = copy(src2, dest);
		}
		if (hasmore1)
			while (hasmore1)
				hasmore1 = copy(src1, dest);
		else
			while (hasmore2)
				hasmore2 = copy(src2, dest);
		src1.close();
		src2.close();
		dest.close();
		return result;
	}

	private boolean copy(Scan src, UpdateScan dest) {
		dest.insert();
		for (String fldname : schema.fields())
			dest.setVal(fldname, src.getVal(fldname));
		return src.next();
	}

	@Override
	public String toString() {
		String c = p.toString();
		String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->");
		sb.append("SortPlan (#blks=" + blocksAccessed() + ", #recs="
				+ recordsOutput() + ")\n");
		for (String child : cs)
			sb.append("\t").append(child).append("\n");
		;
		return sb.toString();
	}
}
