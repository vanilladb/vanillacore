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
package org.vanilladb.core.sql.predicate;

import static org.vanilladb.core.sql.predicate.Term.OP_EQ;
import static org.vanilladb.core.sql.predicate.Term.OP_GT;
import static org.vanilladb.core.sql.predicate.Term.OP_GTE;
import static org.vanilladb.core.sql.predicate.Term.OP_LT;
import static org.vanilladb.core.sql.predicate.Term.OP_LTE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.predicate.Term.Operator;

/**
 * A Boolean combination of terms.
 */
public class Predicate {
	private Collection<Term> terms = new ArrayList<Term>();

	/**
	 * Creates an empty predicate, corresponding to "true".
	 */
	public Predicate() {
	}

	/**
	 * Creates a predicate containing a single term.
	 * 
	 * @param t
	 *            the term
	 */
	public Predicate(Term t) {
		terms.add(t);
	}

	/**
	 * Modifies the predicate to be the conjunction of itself and the specified
	 * term.
	 * 
	 * @param t
	 *            the term to conjunct with
	 */
	public void conjunctWith(Term t) {
		terms.add(t);
	}

	/**
	 * Returns true if this predicate evaluates to true with respect to the
	 * specified record.
	 * 
	 * @param rec
	 *            the record
	 * @return true if the predicate evaluates to true
	 */
	public boolean isSatisfied(Record rec) {
		for (Term t : terms)
			if (!t.isSatisfied(rec))
				return false;
		return true;
	}

	/**
	 * Returns the sub-predicate that applies to the specified schema.
	 * 
	 * @param sch
	 *            the schema
	 * @return the sub-predicate applying to the schema
	 */
	public Predicate selectPredicate(Schema sch) {
		Predicate result = new Predicate();
		for (Term t : terms)
			if (t.isApplicableTo(sch))
				result.terms.add(t);
		if (result.terms.size() == 0)
			return null;
		else
			return result;
	}

	/**
	 * Returns the sub-predicate consisting of terms that applies to the union
	 * of the two specified schemas, but not to either schema separately.
	 * 
	 * @param sch1
	 *            the first schema
	 * @param sch2
	 *            the second schema
	 * @return the sub-predicate whose terms apply to the union of the two
	 *         schemas but not either schema separately.
	 */
	public Predicate joinPredicate(Schema sch1, Schema sch2) {
		Predicate result = new Predicate();
		Schema newsch = new Schema();
		newsch.addAll(sch1);
		newsch.addAll(sch2);
		for (Term t : terms)
			if (!t.isApplicableTo(sch1) && !t.isApplicableTo(sch2)
					&& t.isApplicableTo(newsch))
				result.terms.add(t);
		return result.terms.size() == 0 ? null : result;
	}

	/**
	 * Determines if the specified field is constrained by a constant range in
	 * this predicate. If so, the method returns that range. If not, the method
	 * returns null.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return either the constant range or null
	 */
	public ConstantRange constantRange(String fldName) {
		ConstantRange cr = null;
		for (Term t : terms) {
			Constant c = t.oppositeConstant(fldName);
			if (c != null) {
				Operator op = t.operator(fldName);
				if (op == OP_GT)
					cr = cr == null ? ConstantRange.newInstance(c, false, null,
							false) : cr.applyLow(c, false);
				else if (op == OP_GTE)
					cr = cr == null ? ConstantRange.newInstance(c, true, null,
							false) : cr.applyLow(c, true);
				else if (op == OP_EQ)
					cr = cr == null ? ConstantRange.newInstance(c) : cr
							.applyConstant(c);
				else if (op == OP_LTE)
					cr = cr == null ? ConstantRange.newInstance(null, false, c,
							true) : cr.applyHigh(c, true);
				else if (op == OP_LT)
					cr = cr == null ? ConstantRange.newInstance(null, false, c,
							false) : cr.applyHigh(c, false);
			}
		}
		// validate the constant range
		if (cr != null && cr.isValid()
				&& (cr.hasLowerBound() || cr.hasUpperBound()))
			return cr;
		return null;
	}

	/**
	 * Determines if there are terms of the form "F1=F2" or result in "F1=F2"
	 * via equal transitivity, where F1 is the specified field and F2 is another
	 * field (called join field). If so, the method returns the names of all
	 * join fields. If not, the method returns null.
	 * 
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return the names of the join fields, or null
	 */
	public Set<String> joinFields(String fldName) {
		Set<String> flds = new HashSet<String>();
		flds.add(fldName);
		Deque<String> queue = new LinkedList<String>();
		queue.addLast(fldName);
		/*
		 * Handles the equal transitivity. For example, join fields of F1 in the
		 * predicate "F1=F3 and F3=F4 and F4=F7" are F3, F4 and F7.
		 */
		while (!queue.isEmpty()) {
			String fld = queue.removeFirst();
			for (Term t : terms) {
				String s = t.oppositeField(fld);
				if (s != null && t.operator(fldName) == OP_EQ
						&& !flds.contains(s)) {
					flds.add(s);
					queue.addLast(s);
				}
			}
		}
		flds.remove(fldName);
		return flds.size() == 0 ? null : flds;
	}

	public String toString() {
		Iterator<Term> iter = terms.iterator();
		if (!iter.hasNext())
			return "";
		String result = iter.next().toString();
		while (iter.hasNext())
			result += " and " + iter.next().toString();
		return result;
	}
}
