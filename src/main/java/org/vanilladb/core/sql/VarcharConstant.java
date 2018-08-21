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

import java.nio.charset.Charset;

/**
 * The class that wraps Java strings as database constants.
 */
public class VarcharConstant extends Constant {
	private Type type;
	private String val;

	/**
	 * Create a constant by wrapping the specified string.
	 * 
	 * @param s
	 *            the string value
	 */
	public VarcharConstant(String s) {
		type = new VarcharType(s.length());
		val = s;
	}

	public VarcharConstant(byte[] v, Type type) {
		this.type = type;
		val = new String(v, Charset.forName(VarcharType.CHAR_SET));
	}

	public VarcharConstant(String s, Type type) {
		this.type = type;
		val = s;
	}

	@Override
	public Type getType() {
		return type;
	}

	/**
	 * Unwraps the string and returns it.
	 * 
	 * @see Constant#asJavaVal()
	 */
	@Override
	public Object asJavaVal() {
		return val;
	}

	/**
	 * Each char is encoded using the {@link VarcharType#CHAR_SET}.
	 */
	@Override
	public byte[] asBytes() {
		return val.getBytes(Charset.forName(VarcharType.CHAR_SET));
	}

	@Override
	public int size() {
		return asBytes().length;
	}

	@Override
	public Constant castTo(Type type) {
		if (getType().equals(type))
			return this;
		if (type.getSqlType() != java.sql.Types.VARCHAR
				|| size() > type.maxSize())
			throw new IllegalArgumentException();
		return new VarcharConstant(val, type);
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
		if (!(c instanceof VarcharConstant))
			throw new IllegalArgumentException();
		VarcharConstant sc = (VarcharConstant) c;
		return val.compareTo(sc.val);
	}

	@Override
	public int hashCode() {
		return val.hashCode();
	}

	@Override
	public String toString() {
		return val;
	}

	@Override
	public Constant add(Constant c) {
		if (!(c instanceof VarcharConstant))
			throw new IllegalArgumentException();
		return new VarcharConstant(val + (String) c.asJavaVal());
	}

	@Override
	public Constant sub(Constant c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Constant div(Constant c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Constant mul(Constant c) {
		throw new UnsupportedOperationException();
	}
}
