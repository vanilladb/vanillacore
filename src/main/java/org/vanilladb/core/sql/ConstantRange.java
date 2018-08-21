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
package org.vanilladb.core.sql;

import static org.vanilladb.core.sql.Type.DOUBLE;
import static org.vanilladb.core.sql.Type.VARCHAR;

/**
 * A range of {@link Constant constants}. Instance are immutable.
 */
public abstract class ConstantRange {
	/**
	 * Constructs a new instance. If the bounds are numeric, they will be
	 * converted to {@link DoubleConstant}s and an instance of
	 * {@link DoubleConstantRange} will be returned. Otherwise of the bounds are
	 * of string types, an instance of {@link VarcharConstantRange} will be
	 * returned.
	 * 
	 * @param low
	 *            the lower bound. <code>null</code> means unbound. Note that
	 *            <code>low</code> and <code>high</code> cannot be both
	 *            <code>null</code>.
	 * @param lowIncl
	 *            whether the lower bound is inclusive
	 * @param high
	 *            the higher bound. <code>null</code> means unbound. Note that
	 *            <code>low</code> and <code>high</code> cannot be both
	 *            <code>null</code>.
	 * @param highIncl
	 *            whether the higher bound is inclusive
	 * @return a new instance
	 */
	public static ConstantRange newInstance(Constant low, boolean lowIncl,
			Constant high, boolean highIncl) {
		if (low != null && high != null
				&& !low.getType().equals(high.getType()))
			throw new IllegalArgumentException();

		Type type = low != null ? low.getType() : high.getType();
		if (type.isNumeric()) {
			DoubleConstant lowDouble = low != null ? (DoubleConstant) low
					.castTo(DOUBLE) : null;
			DoubleConstant highDouble = high != null ? (DoubleConstant) high
					.castTo(DOUBLE) : null;
			return new DoubleConstantRange(lowDouble, lowIncl, highDouble,
					highIncl);
		} else if (type.getSqlType() == VARCHAR.getSqlType()) {
			VarcharConstant lowVarchar = low != null ? (VarcharConstant) low
					: null;
			VarcharConstant highVarchar = high != null ? (VarcharConstant) high
					: null;
			return new VarcharConstantRange(lowVarchar, lowIncl,
					(VarcharConstant) highVarchar, highIncl);
		}

		throw new UnsupportedOperationException();
	}

	public static ConstantRange newInstance(Constant c) {
		return newInstance(c, true, c, true);
	}

	/*
	 * Getters
	 */

	/**
	 * Returns whether it is possible to have normal values (e.g., those other
	 * than positive and negative infinities, etc.) lying within this range.
	 * 
	 * @return true if there is possible value
	 */
	public abstract boolean isValid();

	public abstract boolean hasLowerBound();

	public abstract boolean hasUpperBound();

	/**
	 * Returns the lower bound. Note that due to the
	 * {@link #newInstance(Constant, boolean, Constant, boolean) automatic type
	 * conversion}, the returned constant may have a type different from that of
	 * the constant passed at construction time.
	 * 
	 * @return the lower bound if any, or an exception will be fired
	 */
	public abstract Constant low();

	/**
	 * Returns the higher bound. Note that due to the
	 * {@link #newInstance(Constant, boolean, Constant, boolean) automatic type
	 * conversion}, the returned constant may have a type different from that of
	 * the constant passed at construction time.
	 * 
	 * @return the higher bound if any, or an exception will be fired
	 */
	public abstract Constant high();

	public abstract boolean isLowInclusive();

	public abstract boolean isHighInclusive();

	public abstract double length();

	/*
	 * Constant operations.
	 */

	/**
	 * Returns a new range with low set to the specified constant if doing so
	 * makes the lower bound more strict.
	 * 
	 * @param c
	 *            the specified constant
	 * @param inclusive
	 *            whether the constant is inclusive
	 * @return a new range instance with equal or more strict lower bound
	 */
	public abstract ConstantRange applyLow(Constant c, boolean inclusive);

	/**
	 * Returns a new range with high set to the specified constant if doing so
	 * makes the upper bound more strict.
	 * 
	 * @param c
	 *            the specified constant
	 * @param incl
	 *            whether the constant is inclusive
	 * @return a new range instance with equal or more strict upper bound
	 */
	public abstract ConstantRange applyHigh(Constant c, boolean incl);

	public abstract ConstantRange applyConstant(Constant c);

	public abstract boolean isConstant();

	public abstract Constant asConstant();

	public abstract boolean contains(Constant c);

	public abstract boolean lessThan(Constant c);

	public abstract boolean largerThan(Constant c);

	/*
	 * Range operations.
	 */

	public abstract boolean isOverlapping(ConstantRange r);

	public abstract boolean contains(ConstantRange r);

	public abstract ConstantRange intersect(ConstantRange r);

	public abstract ConstantRange union(ConstantRange r);

	/*
	 * Overrides.
	 */

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(isLowInclusive() ? '[' : '(');
		sb.append(low());
		sb.append(", ");
		sb.append(high());
		sb.append(isHighInclusive() ? ']' : ')');
		return sb.toString();
	}
}
