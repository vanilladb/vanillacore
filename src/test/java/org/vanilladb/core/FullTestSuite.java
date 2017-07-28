package org.vanilladb.core;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.vanilladb.core.IsolatedClassLoaderSuite.IsolationRoot;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.FileMgr;

@RunWith(IsolatedClassLoaderSuite.class)
@SuiteClasses({
	StorageTestSuite.class,
	
	QueryTestSuite.class,
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