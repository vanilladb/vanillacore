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
package org.vanilladb.core.storage.file.io.jaydio;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.storage.file.io.IoBuffer;
import org.vanilladb.core.storage.file.io.IoChannel;

import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import net.smacke.jaydio.channel.BufferedChannel;
import net.smacke.jaydio.channel.DirectIoByteChannel;

/*
 * Optimization:
 * This channel will append 100_000 blocks at once if there is no empty blocks for insertion.
 * However, we need to hide the information of redundant blocks and pretend to only append one block
 * because the upper level depends on the file size that has no empty blocks.
 * 
 * Notice:
 * This IoChannel is an EXPERIMENTAL version and CANNOT be merged into the main branch.
 * Some information will be lost if we restart the server,
 * so this IoChannel should collaborate with Auto-bencher because Auto-bencher will reset the record files
 * whenever we start the experiments.
 */
public class JaydioDirectIoChannel implements IoChannel {
	// There will be a lot of empty blocks if append happens
	// We keep a limited file size that is equal to the size of non-empty blocks + 1
	private static ConcurrentHashMap<String, Long> pretendFileSizes = new ConcurrentHashMap<String, Long>();

	private BufferedChannel<AlignedDirectByteBuffer> fileChannel;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	// Optimization: store the size of each table
	// realFileSize considers all empty blocks
	private long realFileSize;
	private String fileName;
	private int singleAppendSize;

	public JaydioDirectIoChannel(File file) throws IOException {
		fileChannel = DirectIoByteChannel.getChannel(file, false);
		fileName = file.getName();
		
		realFileSize = fileChannel.size();
		pretendFileSizes.put(fileName, realFileSize);
		singleAppendSize = 0;
	}

	@Override
	public int read(IoBuffer buffer, long position) throws IOException {
		lock.readLock().lock();
		try {
			JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
			return fileChannel.read(jaydioBuffer.getAlignedDirectByteBuffer(), position);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public int write(IoBuffer buffer, long position) throws IOException {
		lock.writeLock().lock();
		try {
			JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
			int writeSize = fileChannel.write(jaydioBuffer.getAlignedDirectByteBuffer(), position);

			long expectedSize = position + writeSize;
			// Check if an extra block is needed for this write.
			if (expectedSize > pretendFileSizes.get(fileName)) {
				pretendFileSizes.put(fileName, expectedSize);
			}
			
			if (expectedSize > realFileSize) {
				realFileSize = expectedSize;
			}

			return writeSize;
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public long append(IoBuffer buffer) throws IOException {
		lock.writeLock().lock();
		try {
			JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
			
			Long prevFileSize = pretendFileSizes.get(fileName);
			
			// If there are no more empty blocks, append multiple blocks.
			if (prevFileSize == realFileSize) {
				// Append only 1 block, this behavior is as normal.
				int appendOnceSize = 1;
				for (int i = 0; i < appendOnceSize; i++) {
					int tmpAppendSize = fileChannel.write(jaydioBuffer.getAlignedDirectByteBuffer(), realFileSize);
					if (singleAppendSize == 0) {
						singleAppendSize = tmpAppendSize;
					}
					realFileSize += singleAppendSize;
				}
			}
			
			// Pretend we only append a block!!!
			Long pretendFileSize = prevFileSize + singleAppendSize;
			pretendFileSizes.put(fileName, pretendFileSize);
			
			// Tells a lie
			return pretendFileSize;
			
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public long size() throws IOException {
		lock.readLock().lock();
		try {
			return pretendFileSizes.get(fileName);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public void close() throws IOException {
		lock.writeLock().lock();
		try {
			fileChannel.close();
		} finally {
			lock.writeLock().unlock();
		}
	}
}
