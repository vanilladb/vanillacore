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
package org.vanilladb.core.util;

public class CoreProperties extends PropertiesLoader {

	private static CoreProperties loader;
	
	public static CoreProperties getLoader() {
		// Singleton
		if (loader == null)
			loader = new CoreProperties();
		return loader;
	}
	
	protected CoreProperties() {
		super();
	}
	
	@Override
	protected String getConfigFilePath() {
		String path = System.getProperty("org.vanilladb.core.config.file");
		if (path == null || path.isEmpty()) {
			path = "properties/org/vanilladb/core/vanilladb.properties";
		}
		return path;
	}

}
