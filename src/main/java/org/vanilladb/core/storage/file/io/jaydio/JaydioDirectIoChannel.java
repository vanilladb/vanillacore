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

public class JaydioDirectIoChannel implements IoChannel {
	// There will be a lot of empty blocks if append happens
	// We keep a fake file size that is equal to the size of non-empty blocks + 1
	private static ConcurrentHashMap<String, Long> fakeFileSizes = new ConcurrentHashMap<String, Long>();

	private BufferedChannel<AlignedDirectByteBuffer> fileChannel;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	// Optimization: store the size of each table
	// realFileSize includes all empty blocks
	private long realFileSize;
	private String fileName;
	private int appendSize;

	public JaydioDirectIoChannel(File file) throws IOException {
		fileChannel = DirectIoByteChannel.getChannel(file, false);
		fileName = file.getName();
		
		realFileSize = fileChannel.size();
		fakeFileSizes.put(fileName, realFileSize);
		appendSize = 0;
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

			// Check if we need to update the size
			if (position + writeSize > fakeFileSizes.get(fileName)) {
				fakeFileSizes.put(fileName, position + writeSize);
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
			
			Long prevFileSize = fakeFileSizes.get(fileName);
			
			// if there are no more empty blocks, append multiple blocks.
			if (prevFileSize == realFileSize) {
				// Test code: append 100_000 blocks (~400 MB)
				for (int i = 0; i < 100_000; i++) {
					int tmpAppendSize = fileChannel.write(jaydioBuffer.getAlignedDirectByteBuffer(), realFileSize);
					if (appendSize == 0) {
						appendSize = tmpAppendSize;
					}
					realFileSize += appendSize;
				}
			}
			
			Long fakeFileSize = prevFileSize + appendSize;
			fakeFileSizes.put(fileName, fakeFileSize);
			
			// tells a lie
			return fakeFileSize;
			
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public long size() throws IOException {
		lock.readLock().lock();
		try {
			return fakeFileSizes.get(fileName);
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
