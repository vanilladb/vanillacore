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
package org.vanilladb.core.storage.index;

/**
 * Supported index types.
 */
public enum IndexType {
	HASH, BTREE;
	
	public static IndexType fromInteger(int typeVal) {
		switch (typeVal) {
		case 0:
			return HASH;
		case 1:
			return BTREE;
		}
		throw new UnsupportedOperationException();
	}
	
	public int toInteger() {
		switch (this) {
		case HASH:
			return 0;
		case BTREE:
			return 1;
		}
		throw new UnsupportedOperationException();
	}
}
