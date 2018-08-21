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

import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.VARCHAR;

import org.vanilladb.core.util.ByteHelper;

/**
 * The class that wraps Java ints as database constants.
 */
public class IntegerConstant extends Constant {
	private Integer val;

	/**
	 * Create a constant by wrapping the specified int.
	 * 
	 * @param n
	 *            the int value
	 */
	public IntegerConstant(int n) {
		val = n;
	}

	public IntegerConstant(byte[] v) {
		val = ByteHelper.toInteger(v);
	}

	/**
	 * Unwraps the Integer and returns it.
	 * 
	 * @see Constant#asJavaVal()
	 */
	@Override
	public Object asJavaVal() {
		return val;
	}

	@Override
	public Type getType() {
		return Type.INTEGER;
	}

	@Override
	public int size() {
		return ByteHelper.INT_SIZE;
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
		case BIGINT:
			return new BigIntConstant(val.longValue());
		case DOUBLE:
			return new DoubleConstant(val.doubleValue());
		case VARCHAR:
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
			return val.compareTo((Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			Long l = val.longValue();
			return l.compareTo((Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			Double d = val.doubleValue();
			return d.compareTo((Double) c.asJavaVal());
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
			return new IntegerConstant(val + (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			Long l = val.longValue();
			return new BigIntConstant(l + (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			Double d = val.doubleValue();
			return new DoubleConstant(d + (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public Constant sub(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return new IntegerConstant(val - (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			Long l = val.longValue();
			return new BigIntConstant(l - (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			Double d = val.doubleValue();
			return new DoubleConstant(d - (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public Constant div(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return new IntegerConstant(val / (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			Long l = val.longValue();
			return new BigIntConstant(l / (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			Double d = val.doubleValue();
			return new DoubleConstant(d / (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public Constant mul(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return new IntegerConstant(val * (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			Long l = val.longValue();
			return new BigIntConstant(l * (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			Double d = val.doubleValue();
			return new DoubleConstant(d * (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

}
