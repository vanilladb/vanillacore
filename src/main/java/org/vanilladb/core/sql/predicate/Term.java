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

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;

/**
 * A comparison between two expressions.
 */
public class Term {
	public abstract static class Operator {
		abstract Operator complement();

		abstract boolean isSatisfied(Expression lhs, Expression rhs, Record rec);
	}

	public static final Operator OP_EQ = new Operator() {
		@Override
		Operator complement() {
			return OP_EQ;
		}

		@Override
		boolean isSatisfied(Expression lhs, Expression rhs, Record rec) {
			return lhs.evaluate(rec).equals(rhs.evaluate(rec));
		}

		@Override
		public String toString() {
			return "=";
		}
	};

	public static final Operator OP_LT = new Operator() {
		@Override
		Operator complement() {
			return OP_GT;
		}

		@Override
		boolean isSatisfied(Expression lhs, Expression rhs, Record rec) {
			return lhs.evaluate(rec).compareTo(rhs.evaluate(rec)) < 0;
		}

		@Override
		public String toString() {
			return "<";
		}
	};

	public static final Operator OP_LTE = new Operator() {
		@Override
		Operator complement() {
			return OP_GTE;
		}

		@Override
		boolean isSatisfied(Expression lhs, Expression rhs, Record rec) {
			return lhs.evaluate(rec).compareTo(rhs.evaluate(rec)) <= 0;
		}

		@Override
		public String toString() {
			return "<=";
		}
	};

	public static final Operator OP_GT = new Operator() {
		@Override
		Operator complement() {
			return OP_LT;
		}

		@Override
		boolean isSatisfied(Expression lhs, Expression rhs, Record rec) {
			return complement().isSatisfied(rhs, lhs, rec);
		}

		@Override
		public String toString() {
			return ">";
		}
	};

	public static final Operator OP_GTE = new Operator() {
		@Override
		Operator complement() {
			return OP_LTE;
		}

		@Override
		boolean isSatisfied(Expression lhs, Expression rhs, Record rec) {
			return complement().isSatisfied(rhs, lhs, rec);
		}

		@Override
		public String toString() {
			return ">=";
		}
	};

	private Operator op;
	private Expression lhs, rhs;

	public Term(Expression lhs, Operator op, Expression rhs) {
		this.lhs = lhs;
		this.op = op;
		this.rhs = rhs;
	}

	/**
	 * Determines if this term is of the form "F&lt;OP&gt;E" where F is the
	 * specified field, &lt;OP&gt; is an operator, and E is an expression. If
	 * so, the method returns &lt;OP&gt;. If not, the method returns null.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return either the operator or null
	 */
	public Operator operator(String fldName) {
		if (lhs.isFieldName() && lhs.asFieldName().equals(fldName))
			return op;
		if (rhs.isFieldName() && rhs.asFieldName().equals(fldName))
			return op.complement();
		return null;
	}

	/**
	 * Determines if this term is of the form "F&lt;OP&gt;C" where F is the
	 * specified field, &lt;OP&gt; is an operator, and C is some constant. If
	 * so, the method returns C. If not, the method returns null.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return either the constant or null
	 */
	public Constant oppositeConstant(String fldName) {
		if (lhs.isFieldName() && lhs.asFieldName().equals(fldName)
				&& rhs.isConstant())
			return rhs.asConstant();
		if (rhs.isFieldName() && rhs.asFieldName().equals(fldName)
				&& lhs.isConstant())
			return lhs.asConstant();
		return null;
	}

	/**
	 * Determines if this term is of the form "F1&lt;OP&gt;F2" where F1 is the
	 * specified field, &lt;OP&gt; is an operator, and F2 is another field. If
	 * so, the method returns F2. If not, the method returns null.
	 * 
	 * @param fldName
	 *            the name of F1
	 * @return either the name of the other field, or null
	 */
	public String oppositeField(String fldName) {
		if (lhs.isFieldName() && lhs.asFieldName().equals(fldName)
				&& rhs.isFieldName())
			return rhs.asFieldName();
		if (rhs.isFieldName() && rhs.asFieldName().equals(fldName)
				&& lhs.isFieldName())
			return lhs.asFieldName();
		return null;
	}

	/**
	 * Returns true if both expressions of this term apply to the specified
	 * schema.
	 * 
	 * @param sch
	 *            the schema
	 * @return true if both expressions apply to the schema
	 */
	public boolean isApplicableTo(Schema sch) {
		return lhs.isApplicableTo(sch) && rhs.isApplicableTo(sch);
	}

	/**
	 * Returns true if, given the specified record, the two expressions evaluate
	 * to matching values.
	 * 
	 * @param rec
	 *            the record
	 * @return true if both expressions evaluate to matching values based on the
	 *         record
	 */
	public boolean isSatisfied(Record rec) {
		return op.isSatisfied(lhs, rhs, rec);
	}

	public String toString() {
		return lhs.toString() + op.toString() + rhs.toString();
	}
}
