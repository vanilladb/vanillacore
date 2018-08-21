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
package org.vanilladb.core.query.parse;

/**
 * Data for the SQL <em>drop view</em> statement.
 */
public class DropViewData {
	private String viewName;

	/**
	 * Saves the view name and its definition.
	 * 
	 * @param viewName
	 *            the name of the new view
	 */
	public DropViewData(String viewName) {
		this.viewName = viewName;
	}

	/**
	 * Returns the name of the new view.
	 * 
	 * @return the name of the new view
	 */
	public String viewName() {
		return viewName;
	}
}
