/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.vanilladb.core;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.vanilladb.core.IsolatedClassLoaderSuite.IsolationRoot;
import org.vanilladb.core.query.algebra.BasicQueryTest;
import org.vanilladb.core.query.algebra.index.MultiKeyIndexTest;
import org.vanilladb.core.query.parse.ParserTest;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.buffer.BufferConcurrencyTest;
import org.vanilladb.core.storage.buffer.BufferMgrConcurrencyTest;
import org.vanilladb.core.storage.buffer.BufferPoolConcurrencyTest;
import org.vanilladb.core.storage.buffer.BufferTest;
import org.vanilladb.core.storage.buffer.LastLSNTest;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.file.FileTest;
import org.vanilladb.core.storage.file.PageConcurrencyTest;
import org.vanilladb.core.storage.index.btree.BTreeIndexTest;
import org.vanilladb.core.storage.index.btree.BTreeLeafTest;
import org.vanilladb.core.storage.index.btree.BTreePageTest;
import org.vanilladb.core.storage.index.hash.HashIndexTest;
import org.vanilladb.core.storage.metadata.CatalogTest;
import org.vanilladb.core.storage.metadata.statistics.HistogramTest;
import org.vanilladb.core.storage.record.RecordTest;
import org.vanilladb.core.storage.tx.TxTest;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyTest;
import org.vanilladb.core.storage.tx.concurrency.LockTableTest;
import org.vanilladb.core.storage.tx.recovery.RecoveryBasicTest;

@RunWith(IsolatedClassLoaderSuite.class)
@SuiteClasses({
	
	// storage.file
	FileTest.class, PageConcurrencyTest.class,
	
	// storage.buffer
	BufferTest.class, BufferConcurrencyTest.class,
	BufferMgrConcurrencyTest.class, BufferPoolConcurrencyTest.class,
	LastLSNTest.class,
	
	// storage.record
	RecordTest.class,
	
	// storage.metadata
	CatalogTest.class,
	
	// storage.metadata
	HistogramTest.class,
	
	// storage.index.btree
	BTreeIndexTest.class, BTreeLeafTest.class,
	BTreePageTest.class,
	
	// storage.index.hash
	HashIndexTest.class,
	
	// storage.tx
	TxTest.class,
	
	// storage.tx.concurrency
	ConcurrencyTest.class, LockTableTest.class,
	
	// storage.tx.recovery
	RecoveryBasicTest.class,
	
	// query.parse
	ParserTest.class,
	
	// query.algebra
	BasicQueryTest.class, MultiKeyIndexTest.class,
	
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
