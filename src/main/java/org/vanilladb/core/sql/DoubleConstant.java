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

import org.vanilladb.core.util.ByteHelper;

/**
 * The class that wraps Java doubles as database constants.
 */
public class DoubleConstant extends Constant {
	private Double val;

	/**
	 * Create a constant by wrapping the specified long.
	 * 
	 * @param n
	 *            the long value
	 */
	public DoubleConstant(double n) {
		val = n;
	}

	public DoubleConstant(byte[] v) {
		val = ByteHelper.toDouble(v);
	}

	/**
	 * Unwraps the Double and returns it.
	 * 
	 * @see Constant#asJavaVal()
	 */
	@Override
	public Object asJavaVal() {
		return val;
	}

	@Override
	public Type getType() {
		return Type.DOUBLE;
	}

	@Override
	public int size() {
		return ByteHelper.DOUBLE_SIZE;
	}

	@Override
	public byte[] asBytes() {
		return ByteHelper.toBytes(val);
	}

	@Override
	public Constant castTo(Type type) {
		if (getType().equals(type))
			return this;
		switch (type.getSqlType()) {
		case java.sql.Types.INTEGER:
			return new IntegerConstant(val.intValue());
		case java.sql.Types.BIGINT:
			return new BigIntConstant(val.longValue());
		case java.sql.Types.VARCHAR:
			return new VarcharConstant(val.toString(), type);
		}
		throw new IllegalArgumentException("Unspported constant type");
	}

	/**
	 * Indicates whether some other object is {@link Constant} object and its
	 * value equal to this one.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		return compareTo((Constant) obj) == 0;
	}

	@Override
	public int compareTo(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return val.compareTo(((Integer) c.asJavaVal()).doubleValue());
		else if (c instanceof BigIntConstant)
			return val.compareTo(((Long) c.asJavaVal()).doubleValue());
		else if (c instanceof DoubleConstant) {
			return val.compareTo((Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public int hashCode() {
		return val.hashCode();
	}

	@Override
	public String toString() {
		return val.toString();
	}

	@Override
	public Constant add(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return new DoubleConstant(val + (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			return new DoubleConstant(val + (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			return new DoubleConstant(val + (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public Constant sub(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return new DoubleConstant(val - (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			return new DoubleConstant(val - (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			return new DoubleConstant(val - (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public Constant div(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return new DoubleConstant(val / (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			return new DoubleConstant(val / (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			return new DoubleConstant(val / (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public Constant mul(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return new DoubleConstant(val * (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			return new DoubleConstant(val * (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			return new DoubleConstant(val * (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}
}
