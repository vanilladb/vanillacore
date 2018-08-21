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

public class DoubleType extends Type {
	DoubleType() {
	}

	@Override
	public int getSqlType() {
		return java.sql.Types.DOUBLE;
	}

	@Override
	public int getArgument() {
		return -1;
	}

	@Override
	public boolean isFixedSize() {
		return true;
	}

	@Override
	public boolean isNumeric() {
		return true;
	}

	@Override
	public int maxSize() {
		return ByteHelper.DOUBLE_SIZE;
	}

	@Override
	public Constant maxValue() {
		return new DoubleConstant(Double.MAX_VALUE);
	}

	@Override
	public Constant minValue() {
		return new DoubleConstant(Double.MIN_VALUE);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj instanceof DoubleType))
			return false;
		DoubleType t = (DoubleType) obj;
		return getSqlType() == t.getSqlType()
				&& getArgument() == t.getArgument();
	}

	@Override
	public String toString() {
		return "DOUBLE";
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}
}
