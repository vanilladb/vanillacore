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

/**
 * An abstract class that denotes a supported SQL type.
 */
public abstract class Type {
	public static final Type INTEGER = new IntegerType();
	public static final Type BIGINT = new BigIntType();
	public static final Type DOUBLE = new DoubleType();
	public static final Type VARCHAR = new VarcharType();

	public static Type VARCHAR(int arg) {
		return new VarcharType(arg);
	};

	/**
	 * Constructs a new instance corresponding to the specified SQL type and
	 * argument.
	 * 
	 * @param sqlType
	 *            the corresponding SQL type
	 * @param arg
	 *            the SQL type argument. E.g., VARCHAR(20) has argument 20.
	 * @return a new instance corresponding to the specified SQL type and
	 *         argument.
	 */
	public static Type newInstance(int sqlType, int arg) {
		switch (sqlType) {
		case (java.sql.Types.INTEGER):
			return INTEGER;
		case (java.sql.Types.BIGINT):
			return BIGINT;
		case (java.sql.Types.DOUBLE):
			return DOUBLE;
		case (java.sql.Types.VARCHAR):
			return VARCHAR(arg);
		}
		throw new UnsupportedOperationException("Unspported SQL type: "
				+ sqlType);
	}

	/**
	 * Returns the SQL type corresponding to this instance.
	 * 
	 * @return the corresponding SQL type
	 */
	public abstract int getSqlType();

	/**
	 * Returns the argument associated with this instance. For example, this
	 * methods returns 20 for the SQL type VARCHAR(20).
	 * 
	 * @return the argument associated with this instance
	 */
	public abstract int getArgument();

	/**
	 * Returns whether the number of bytes required to encode {@link Constant
	 * values} of this type is fixed.
	 * 
	 * @return whether the number of bytes of different values is fixed
	 */
	public abstract boolean isFixedSize();

	/**
	 * Returns whether the values of this type is numeric.
	 * 
	 * @return whether the values of this type is numeric
	 */
	public abstract boolean isNumeric();

	/**
	 * Returns the maximum number of bytes required to encode a {@link Constant
	 * value} of this type.
	 * 
	 * @return the number of bytes
	 */
	public abstract int maxSize();

	/**
	 * Returns the maximal {@link Constant value} of this type.
	 * 
	 * @return the maximal value
	 */
	public abstract Constant maxValue();

	/**
	 * Returns the minimal {@link Constant value} of this type.
	 * 
	 * @return the minimal value
	 */
	public abstract Constant minValue();

}
