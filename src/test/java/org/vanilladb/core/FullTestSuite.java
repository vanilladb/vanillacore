/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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
package org.vanilladb.core;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.vanilladb.core.IsolatedClassLoaderSuite.IsolationRoot;
import org.vanilladb.core.integration.PhantomTest;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.FileMgr;

@RunWith(IsolatedClassLoaderSuite.class)
@SuiteClasses({
	StorageTestSuite.class,
	
	QueryTestSuite.class,
	
	// Integration Tests
	PhantomTest.class
})
@IsolationRoot(VanillaDb.class)
public class FullTestSuite {
	@BeforeClass
	public static void init() {
		// Delete the previous test databases
		File mainDir = new File(FileMgr.DB_FILES_DIR, ServerInit.DB_MAIN_DIR);
		if (mainDir.exists())
			delete(mainDir);
	}
	
	private static void delete(File path) {
		if (path.isDirectory()) {
			// Delete the contents
			File[] files = path.listFiles();
			for (File file : files)
				delete(file);
			
			// Delete the empty directory
			if (!path.delete())
				throw new RuntimeException("cannot delete the directory: " + path);
		} else {
			if (!path.delete())
				throw new RuntimeException("cannot delete the file: " + path);
		}
	}
}
