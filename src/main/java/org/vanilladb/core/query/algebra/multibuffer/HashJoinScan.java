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

import static org.vanilladb.core.sql.predicate.Term.OP_EQ;

import java.util.List;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.SelectScan;
import org.vanilladb.core.query.algebra.materialize.TempTable;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.predicate.Expression;
import org.vanilladb.core.sql.predicate.FieldNameExpression;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class HashJoinScan implements Scan {
	private List<TempTable> tables1, tables2;
	private Transaction tx;
	private int currentIndex;
	private Scan current;
	private Predicate pred;

	public HashJoinScan(List<TempTable> tables1, List<TempTable> tables2,
			String fldname1, String fldname2, Transaction tx) {
		this.tables1 = tables1;
		this.tables2 = tables2;
		this.tx = tx;
		Expression exp1 = new FieldNameExpression(fldname1);
		Expression exp2 = new FieldNameExpression(fldname2);
		Term t = new Term(exp1, OP_EQ, exp2);
		pred = new Predicate(t);
	}

	@Override
	public void beforeFirst() {
		openscan(0);
	}

	@Override
	public boolean next() {
		while (true) {
			if (current.next())
				return true;
			currentIndex++;
			if (currentIndex >= tables1.size())
				return false;
			openscan(currentIndex);
		}
	}

	private void openscan(int n) {
		close();
		currentIndex = n;
		Scan s1 = tables1.get(n).open();
		TableInfo ti2 = tables2.get(n).getTableInfo();
		Scan s3 = new MultiBufferProductScan(s1, ti2, tx);
		current = new SelectScan(s3, pred);
	}

	@Override
	public void close() {
		if (current != null)
			current.close();
	}

	@Override
	public Constant getVal(String fldname) {
		return current.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return current.hasField(fldname);
	}
}
