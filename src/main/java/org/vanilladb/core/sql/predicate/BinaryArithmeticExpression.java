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
 * An expression consisting of a binary arithmetic operator and two
 * sub-expressions as operands.
 */
public class BinaryArithmeticExpression implements Expression {
	public abstract static class Operator {
		abstract Constant evaluate(Expression lhs, Expression rhs, Record rec);
	}

	public static final Operator OP_ADD = new Operator() {
		@Override
		Constant evaluate(Expression lhs, Expression rhs, Record rec) {
			return lhs.evaluate(rec).add(rhs.evaluate(rec));
		}

		@Override
		public String toString() {
			return "add";
		}
	};

	public static final Operator OP_SUB = new Operator() {
		@Override
		Constant evaluate(Expression lhs, Expression rhs, Record rec) {
			return lhs.evaluate(rec).sub(rhs.evaluate(rec));
		}

		@Override
		public String toString() {
			return "sub";
		}
	};

	public static final Operator OP_MUL = new Operator() {
		@Override
		Constant evaluate(Expression lhs, Expression rhs, Record rec) {
			return lhs.evaluate(rec).mul(rhs.evaluate(rec));
		}

		@Override
		public String toString() {
			return "mul";
		}
	};

	public static final Operator OP_DIV = new Operator() {
		@Override
		Constant evaluate(Expression lhs, Expression rhs, Record rec) {
			return lhs.evaluate(rec).div(rhs.evaluate(rec));
		}

		@Override
		public String toString() {
			return "div";
		}
	};

	private Operator op;
	private Expression lhs, rhs;

	/**
	 * Creates a binary arithmetic expression by wrapping two expressions and
	 * one operator.
	 * 
	 * @param lhs
	 *            the left hand side expression
	 * @param op
	 *            the operator
	 * @param rhs
	 *            the right hand side expression
	 */
	public BinaryArithmeticExpression(Expression lhs, Operator op,
			Expression rhs) {
		this.lhs = lhs;
		this.op = op;
		this.rhs = rhs;
	}

	/**
	 * Returns true if both expressions are constant.
	 * 
	 * @see Expression#isConstant()
	 */
	@Override
	public boolean isConstant() {
		return lhs.isConstant() && rhs.isConstant();
	}

	/**
	 * Returns false.
	 * 
	 * @see Expression#isFieldName()
	 */
	@Override
	public boolean isFieldName() {
		return false;
	}

	/**
	 * Return the evaluated constant value if both expressions are constant.
	 * 
	 * @see Expression#asConstant()
	 */
	@Override
	public Constant asConstant() {
		if (!isConstant())
			throw new ClassCastException();
		return evaluate((Record) null);
	}

	/**
	 * This method should never be called. Throws a ClassCastException.
	 * 
	 * @see Expression#asFieldName()
	 */
	@Override
	public String asFieldName() {
		throw new ClassCastException();
	}

	/**
	 * Evaluates the arithmetic expression by computing on the values from the
	 * record.
	 * 
	 * @see Expression#evaluate(Record)
	 */
	@Override
	public Constant evaluate(Record rec) {
		return op.evaluate(lhs, rhs, rec);
	}

	/**
	 * Returns true if both expressions are in the specified schema.
	 * 
	 * @see Expression#isApplicableTo(Schema)
	 */
	@Override
	public boolean isApplicableTo(Schema sch) {
		return lhs.isApplicableTo(sch) && rhs.isApplicableTo(sch);
	}

	@Override
	public String toString() {
		return op.toString() + "(" + lhs.toString() + "," + rhs.toString()
				+ ")";
	}
}
