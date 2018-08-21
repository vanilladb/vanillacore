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

public class DoubleConstantRange extends ConstantRange {
	private static DoubleConstant INF = new DoubleConstant(
			Double.POSITIVE_INFINITY);
	private static DoubleConstant NEG_INF = new DoubleConstant(
			Double.NEGATIVE_INFINITY);
	private static DoubleConstant NAN = new DoubleConstant(Double.NaN);

	private DoubleConstant low;
	private DoubleConstant high;
	private boolean lowIncl;
	private boolean highIncl;

	/**
	 * Constructs a new instance.
	 * 
	 * @param low
	 *            the lower bound. <code>null</code> means unbound.
	 * @param lowIncl
	 *            whether the lower bound is inclusive
	 * @param high
	 *            the higher bound. <code>null</code> means unbound.
	 * @param highIncl
	 *            whether the higher bound is inclusive
	 */
	public DoubleConstantRange(Double low, boolean lowIncl, Double high,
			boolean highIncl) {
		if (low == null)
			this.low = NEG_INF;
		else {
			this.low = new DoubleConstant(low);
			this.lowIncl = lowIncl;
		}
		if (high == null)
			this.high = INF;
		else {
			this.high = new DoubleConstant(high);
			this.highIncl = highIncl;
		}
	}

	DoubleConstantRange(DoubleConstant low, boolean lowIncl,
			DoubleConstant high, boolean highIncl) {
		if (low == null)
			this.low = NEG_INF;
		else {
			this.low = low;
			this.lowIncl = lowIncl;
		}
		if (high == null)
			this.high = INF;
		else {
			this.high = high;
			this.highIncl = highIncl;
		}
	}

	/*
	 * Getters
	 */

	/**
	 * Returns whether it is possible to have normal values (i.e., those other
	 * than {@link #NEG_INF}, {@link #INF}, and {@link #NAN}) lying within this
	 * range.
	 */
	@Override
	public boolean isValid() {
		return !low.equals(NAN)
				&& !high.equals(NAN)
				&& (low.compareTo(high) < 0 || (low.compareTo(high) == 0
						&& lowIncl && highIncl));
	}

	@Override
	public boolean hasLowerBound() {
		return !low.equals(NEG_INF);
	}

	@Override
	public boolean hasUpperBound() {
		return !high.equals(INF);
	}

	@Override
	public Constant low() {
		return low;
	}

	@Override
	public Constant high() {
		return high;
	}

	@Override
	public boolean isLowInclusive() {
		return lowIncl;
	}

	@Override
	public boolean isHighInclusive() {
		return highIncl;
	}

	@Override
	public double length() {
		if (!isValid())
			return (Double) NAN.asJavaVal();
		return (Double) high.sub(low).asJavaVal();
	}

	/*
	 * Constant operations.
	 */

	@Override
	public ConstantRange applyLow(Constant c, boolean incl) {
		if (!c.getType().isNumeric())
			throw new IllegalArgumentException();
		DoubleConstant cDouble = (DoubleConstant) c.castTo(DOUBLE);
		if (low.equals(NAN))
			return this;
		DoubleConstant l = low;
		boolean li = lowIncl;
		if (low.compareTo(cDouble) < 0) {
			l = cDouble;
			li = incl;
		} else if (low.compareTo(cDouble) == 0 && lowIncl == true
				&& incl == false) {
			li = false;
		}
		return new DoubleConstantRange(l, li, high, highIncl);
	}

	@Override
	public ConstantRange applyHigh(Constant c, boolean incl) {
		if (!c.getType().isNumeric())
			throw new IllegalArgumentException();
		DoubleConstant cDouble = (DoubleConstant) c.castTo(DOUBLE);
		if (high.equals(NAN))
			return this;
		DoubleConstant h = high;
		boolean hi = highIncl;
		if (high.compareTo(cDouble) > 0) {
			h = cDouble;
			hi = incl;
		} else if (high.compareTo(cDouble) == 0 && highIncl == true
				&& incl == false) {
			hi = false;
		}
		return new DoubleConstantRange(low, lowIncl, h, hi);
	}

	@Override
	public ConstantRange applyConstant(Constant c) {
		if (!c.getType().isNumeric())
			throw new IllegalArgumentException();
		return applyLow(c, true).applyHigh(c, true);
	}

	@Override
	public boolean isConstant() {
		return !low.equals(NAN) && !high.equals(NAN) && !low.equals(NEG_INF)
				&& !high.equals(INF) && low.equals(high) && lowIncl == true
				&& highIncl == true;
	}

	@Override
	public Constant asConstant() {
		if (isConstant())
			return low;
		throw new IllegalStateException();
	}

	@Override
	public boolean contains(Constant c) {
		if (!c.getType().isNumeric())
			throw new IllegalArgumentException();
		DoubleConstant cDouble = (DoubleConstant) c.castTo(DOUBLE);
		if (!isValid())
			return false;
		if ((lowIncl && cDouble.compareTo(low) < 0)
				|| (!lowIncl && cDouble.compareTo(low) <= 0))
			return false;
		if ((highIncl && cDouble.compareTo(high) > 0)
				|| (!highIncl && cDouble.compareTo(high) >= 0))
			return false;
		return true;
	}

	@Override
	public boolean lessThan(Constant c) {
		if (high.compareTo(c) > 0)
			return false;
		else if (high.compareTo(c) == 0 && highIncl)
			return false;
		return true;
	}

	@Override
	public boolean largerThan(Constant c) {
		if (low.compareTo(c) < 0)
			return false;
		else if (low.compareTo(c) == 0 && lowIncl)
			return false;
		return true;
	}

	/*
	 * Range operations.
	 */

	@Override
	public boolean isOverlapping(ConstantRange r) {
		if (!(r instanceof DoubleConstantRange))
			throw new IllegalArgumentException();
		if (!isValid() || !r.isValid())
			return false;
		DoubleConstantRange dr = (DoubleConstantRange) r;
		DoubleConstant rh = dr.high;
		boolean rhi = dr.highIncl;
		if (!low.equals(NEG_INF)
				&& ((lowIncl && ((rhi && rh.compareTo(low) < 0) || (!rhi && rh
						.compareTo(low) <= 0))) || (!lowIncl && rh
						.compareTo(low) <= 0)))
			return false;
		DoubleConstant rl = dr.low;
		boolean rli = dr.lowIncl;
		if (!high.equals(INF)
				&& ((highIncl && ((rli && rl.compareTo(high) > 0) || (!rli && rl
						.compareTo(high) >= 0))) || (!highIncl && rl
						.compareTo(high) >= 0)))
			return false;
		return true;
	}

	@Override
	public boolean contains(ConstantRange r) {
		if (!(r instanceof DoubleConstantRange))
			throw new IllegalArgumentException();
		if (!isValid() || !r.isValid())
			return false;
		DoubleConstantRange dr = (DoubleConstantRange) r;
		DoubleConstant rl = dr.low;
		boolean rli = dr.lowIncl;
		if (!low.equals(NEG_INF)
				&& ((!lowIncl && ((rli && rl.compareTo(low) <= 0) || (!rli && rl
						.compareTo(low) < 0))) || (lowIncl && rl.compareTo(low) < 0)))
			return false;
		DoubleConstant rh = dr.high;
		boolean rhi = dr.highIncl;
		if (!high.equals(INF)
				&& ((!highIncl && ((rhi && rh.compareTo(high) >= 0) || (!rhi && rh
						.compareTo(high) > 0))) || (highIncl && rh
						.compareTo(high) > 0)))
			return false;
		return true;
	}

	@Override
	public ConstantRange intersect(ConstantRange r) {
		if (!(r instanceof DoubleConstantRange))
			throw new IllegalArgumentException();
		DoubleConstantRange dr = (DoubleConstantRange) r;
		DoubleConstant l = null;
		boolean li = false;
		if (low.equals(NAN) || dr.low.equals(NAN))
			l = NAN;
		else {
			l = low.compareTo(dr.low) > 0 ? low : dr.low;
			li = lowIncl;
			if (low.compareTo(dr.low) == 0)
				li &= dr.lowIncl;
			else if (low.compareTo(dr.low) < 0)
				li = dr.lowIncl;
		}
		DoubleConstant h = null;
		boolean hi = false;
		if (high.equals(NAN) || dr.high.equals(NAN))
			h = NAN;
		else {
			h = high.compareTo(dr.high) < 0 ? high : dr.high;
			hi = highIncl;
			if (high.compareTo(dr.high) == 0)
				hi &= dr.highIncl;
			else if (high.compareTo(dr.high) > 0)
				hi = dr.highIncl;
		}
		return new DoubleConstantRange(l, li, h, hi);
	}

	@Override
	public ConstantRange union(ConstantRange r) {
		if (!(r instanceof DoubleConstantRange))
			throw new IllegalArgumentException();
		DoubleConstantRange dr = (DoubleConstantRange) r;
		DoubleConstant l = null;
		boolean li = false;
		if (low.equals(NAN) || dr.low.equals(NAN))
			l = NAN;
		else {
			l = low.compareTo(dr.low) < 0 ? low : dr.low;
			li = lowIncl;
			if (low.compareTo(dr.low) == 0)
				li |= dr.lowIncl;
			else if (low.compareTo(dr.low) > 0)
				li = dr.lowIncl;
		}
		DoubleConstant h = null;
		boolean hi = false;
		if (high.equals(NAN) || dr.high.equals(NAN))
			h = NAN;
		else {
			h = high.compareTo(dr.high) > 0 ? high : dr.high;
			hi = highIncl;
			if (high.compareTo(dr.high) == 0)
				hi |= dr.highIncl;
			else if (high.compareTo(dr.high) < 0)
				hi = dr.highIncl;
		}
		return new DoubleConstantRange(l, li, h, hi);
	}
}
