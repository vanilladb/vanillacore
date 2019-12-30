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
package org.vanilladb.core;

import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.vanilladb.core.IsolatedClassLoaderSuite.IsolationRoot;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.buffer.BufferConcurrencyTest;
import org.vanilladb.core.storage.buffer.BufferMgrConcurrencyTest;
import org.vanilladb.core.storage.buffer.BufferPoolConcurrencyTest;
import org.vanilladb.core.storage.buffer.BufferTest;
import org.vanilladb.core.storage.buffer.LastLSNTest;
import org.vanilladb.core.storage.file.FileTest;
import org.vanilladb.core.storage.file.PageConcurrencyTest;
import org.vanilladb.core.storage.index.btree.BTreeIndexConcurrentTest;
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
	BTreePageTest.class, BTreeIndexConcurrentTest.class,
	
	// storage.index.hash
	HashIndexTest.class,
	
	// storage.tx
	TxTest.class,
	
	// storage.tx.concurrency
	ConcurrencyTest.class, LockTableTest.class,
	
	// storage.tx.recovery
	RecoveryBasicTest.class,
})
@IsolationRoot(VanillaDb.class)
public class StorageTestSuite {
	
}
