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
package org.vanilladb.core.query.planner;

/**
 * A runtime exception indicating that the submitted query has incorrect
 * semantic. For example, the mentioned field or table name in the query is not
 * existed.
 */
@SuppressWarnings("serial")
public class BadSemanticException extends RuntimeException {
	public BadSemanticException() {
	}

	public BadSemanticException(String message) {
		super(message);
	}
}
