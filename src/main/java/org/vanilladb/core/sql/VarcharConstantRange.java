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

import static org.vanilladb.core.sql.Type.VARCHAR;

public class VarcharConstantRange extends ConstantRange {
	private static VarcharConstant NEG_INF = new VarcharConstant("") {
		@Override
		public boolean equals(Object obj) {
			if (obj == this)
				return true;
			if (obj == null || !(obj instanceof VarcharConstant))
				return false;
			return this.compareTo((VarcharConstant) obj) == 0;
		}

		@Override
		public int compareTo(Constant c) {
			if (c == this)
				return 0;
			return -1;
		}

		@Override
		public Type getType() {
			return VARCHAR;
		}

		@Override
		public Object asJavaVal() {
			throw new UnsupportedOperationException();
		}

		@Override
		public byte[] asBytes() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int size() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Constant castTo(Type type) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Constant add(Constant c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Constant sub(Constant c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Constant mul(Constant c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Constant div(Constant c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String toString() {
			return "'-Infinity'";
		}
	};

	private static VarcharConstant INF = new VarcharConstant("") {
		@Override
		public boolean equals(Object obj) {
			if (obj == this)
				return true;
			if (obj == null || !(obj instanceof VarcharConstant))
				return false;
			// no other varchar constants are equal to INF
			return false;
		}

		@Override
		public int compareTo(Constant c) {
			if (c == this)
				return 0;
			return 1;
		}

		@Override
		public Type getType() {
			return VARCHAR;
		}

		@Override
		public Object asJavaVal() {
			throw new UnsupportedOperationException();
		}

		@Override
		public byte[] asBytes() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int size() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Constant castTo(Type type) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Constant add(Constant c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Constant sub(Constant c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Constant mul(Constant c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Constant div(Constant c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String toString() {
			return "'Infinity'";
		}
	};

	private VarcharConstant low;
	private VarcharConstant high;
	private boolean lowIncl, hasLowerBound;
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
	public VarcharConstantRange(String low, boolean lowIncl, String high,
			boolean highIncl) {
		if (low == null)
			this.low = NEG_INF;
		else {
			this.low = new VarcharConstant(low);
			hasLowerBound = true;
			this.lowIncl = lowIncl;
		}
		if (high == null)
			this.high = INF;
		else {
			this.high = new VarcharConstant(high);
			this.highIncl = highIncl;
		}
	}

	VarcharConstantRange(VarcharConstant low, boolean lowIncl,
			VarcharConstant high, boolean highIncl) {
		if (low == null)
			this.low = NEG_INF;
		else {
			this.low = low;
			hasLowerBound = true;
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

	@Override
	public boolean isValid() {
		if (!INF.equals(high))
			return low.compareTo(high) < 0
					|| (low.compareTo(high) == 0 && lowIncl && highIncl);
		else
			return high.compareTo(low) > 0;
	}

	@Override
	public boolean hasLowerBound() {
		return hasLowerBound;
	}

	@Override
	public boolean hasUpperBound() {
		return !INF.equals(high);
	}

	@Override
	public Constant low() {
		if (hasLowerBound)
			return low;
		throw new IllegalStateException();
	}

	@Override
	public Constant high() {
		if (!NEG_INF.equals(high) && !INF.equals(high))
			return high;
		throw new IllegalStateException();
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
		throw new UnsupportedOperationException();
	}

	/*
	 * Constant operations.
	 */

	@Override
	public ConstantRange applyLow(Constant c, boolean incl) {
		if (!(c instanceof VarcharConstant))
			throw new IllegalArgumentException();
		VarcharConstant l = low;
		boolean li = lowIncl;
		if (low.compareTo(c) < 0) {
			l = (VarcharConstant) c;
			li = incl;
		} else if (low.compareTo(c) == 0 && lowIncl == true && incl == false)
			li = false;
		return new VarcharConstantRange(l, li, high, highIncl);
	}

	@Override
	public ConstantRange applyHigh(Constant c, boolean incl) {
		if (!(c instanceof VarcharConstant))
			throw new IllegalArgumentException();
		VarcharConstant h = high;
		boolean hi = highIncl;
		if (high.compareTo(c) > 0) {
			h = (VarcharConstant) c;
			hi = incl;
		} else if (high.compareTo(c) == 0 && highIncl == true && incl == false)
			hi = false;
		return new VarcharConstantRange(low, lowIncl, h, hi);
	}

	@Override
	public ConstantRange applyConstant(Constant c) {
		if (!(c instanceof VarcharConstant))
			throw new IllegalArgumentException();
		return applyLow(c, true).applyHigh(c, true);
	}

	@Override
	public boolean isConstant() {
		// do not use !NINF.equals(low), if low = "", this may goes wrong
		return hasLowerBound && !INF.equals(high) && low.equals(high)
				&& lowIncl == true && highIncl == true;
	}

	@Override
	public Constant asConstant() {
		if (isConstant())
			return low;
		throw new IllegalStateException();
	}

	@Override
	public boolean contains(Constant c) {
		if (!(c instanceof VarcharConstant))
			throw new IllegalArgumentException();
		if (!isValid())
			return false;
		/*
		 * Note that if low and high are INF ore NEG_INF here, using
		 * c.compare(high/low) will have wrong answer.
		 * 
		 * For example, if high=INF, the result of c.compareTo(high) is the same
		 * as c.compareTo("Infinity").
		 */
		if ((lowIncl && low.compareTo(c) > 0)
				|| (!lowIncl && low.compareTo(c) >= 0))
			return false;
		if ((highIncl && high.compareTo(c) < 0)
				|| (!highIncl && high.compareTo(c) <= 0))
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
		if (!(r instanceof VarcharConstantRange))
			throw new IllegalArgumentException();
		if (!isValid() || !r.isValid())
			return false;
		VarcharConstantRange sr = (VarcharConstantRange) r;
		VarcharConstant rh = sr.high;
		boolean rhi = sr.highIncl;
		if (!low.equals(NEG_INF)
				&& ((lowIncl && ((rhi && rh.compareTo(low) < 0) || (!rhi && rh
						.compareTo(low) <= 0))) || (!lowIncl && rh
						.compareTo(low) <= 0)))
			return false;
		VarcharConstant rl = sr.low;
		boolean rli = sr.lowIncl;
		if (!high.equals(INF)
				&& ((highIncl && ((rli && rl.compareTo(high) > 0) || (!rli && rl
						.compareTo(high) >= 0))) || (!highIncl && rl
						.compareTo(high) >= 0)))
			return false;
		return true;
	}

	// TODO : check the possible of c.compareTo(INF)
	@Override
	public boolean contains(ConstantRange r) {
		if (!(r instanceof VarcharConstantRange))
			throw new IllegalArgumentException();
		if (!isValid() || !r.isValid())
			return false;
		VarcharConstantRange sr = (VarcharConstantRange) r;
		VarcharConstant rl = sr.low;
		boolean rli = sr.lowIncl;
		if (!low.equals(NEG_INF)
				&& ((!lowIncl && ((rli && rl.compareTo(low) <= 0) || (!rli && rl
						.compareTo(low) < 0))) || (lowIncl && rl.compareTo(low) < 0)))
			return false;
		VarcharConstant rh = sr.high;
		boolean rhi = sr.highIncl;
		if (!high.equals(INF)
				&& ((!highIncl && ((rhi && rh.compareTo(high) >= 0) || (!rhi && rh
						.compareTo(high) > 0))) || (highIncl && rh
						.compareTo(high) > 0)))
			return false;
		return true;
	}

	@Override
	public ConstantRange intersect(ConstantRange r) {
		if (!(r instanceof VarcharConstantRange))
			throw new IllegalArgumentException();
		VarcharConstantRange sr = (VarcharConstantRange) r;

		VarcharConstant l = low.compareTo(sr.low) > 0 ? low : sr.low;
		boolean li = lowIncl;
		if (low.compareTo(sr.low) == 0)
			li &= sr.lowIncl;
		else if (low.compareTo(sr.low) < 0)
			li = sr.lowIncl;

		VarcharConstant h = high.compareTo(sr.high) < 0 ? high : sr.high;
		boolean hi = highIncl;
		if (high.compareTo(sr.high) == 0)
			hi &= sr.highIncl;
		else if (high.compareTo(sr.high) > 0)
			hi = sr.highIncl;
		return new VarcharConstantRange(l, li, h, hi);
	}

	@Override
	public ConstantRange union(ConstantRange r) {
		if (!(r instanceof VarcharConstantRange))
			throw new IllegalArgumentException();
		VarcharConstantRange sr = (VarcharConstantRange) r;

		VarcharConstant l = low.compareTo(sr.low) < 0 ? low : sr.low;
		boolean li = lowIncl;
		if (low.compareTo(sr.low) == 0)
			li |= sr.lowIncl;
		else if (low.compareTo(sr.low) > 0)
			li = sr.lowIncl;

		VarcharConstant h = high.compareTo(sr.high) > 0 ? high : sr.high;
		boolean hi = highIncl;
		if (high.compareTo(sr.high) == 0)
			hi |= sr.highIncl;
		else if (high.compareTo(sr.high) < 0)
			hi = sr.highIncl;
		return new VarcharConstantRange(l, li, h, hi);
	}
}
