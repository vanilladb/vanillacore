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
package org.vanilladb.core.storage.file.io.javanio;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.storage.file.io.IoBuffer;
import org.vanilladb.core.storage.file.io.IoChannel;

public class JavaNioFileChannel implements IoChannel {

	private FileChannel fileChannel;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	// Optimization: store the size of each table
	private long fileSize;

	public JavaNioFileChannel(File file) throws IOException {
		@SuppressWarnings("resource")
		RandomAccessFile f = new RandomAccessFile(file, "rws");
		fileChannel = f.getChannel();
		fileSize = fileChannel.size();
	}

	@Override
	public int read(IoBuffer buffer, long position) throws IOException {
		lock.readLock().lock();
		try {
			JavaNioByteBuffer javaBuffer = (JavaNioByteBuffer) buffer;
			return fileChannel.read(javaBuffer.getByteBuffer(), position);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public int write(IoBuffer buffer, long position) throws IOException {
		lock.writeLock().lock();
		try {
			JavaNioByteBuffer javaBuffer = (JavaNioByteBuffer) buffer;
			int writeSize = fileChannel.write(javaBuffer.getByteBuffer(), position);
			
			// Check if we need to update the size
			if (position + writeSize > fileSize)
				fileSize = position + writeSize;
			
			return writeSize;
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public long append(IoBuffer buffer) throws IOException {
		lock.writeLock().lock();
		try {
			JavaNioByteBuffer javaBuffer = (JavaNioByteBuffer) buffer;
			int appendSize = fileChannel.write(javaBuffer.getByteBuffer(), fileSize);
			fileSize += appendSize;
			return fileSize;
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public long size() throws IOException {
		lock.readLock().lock();
		try {
			return fileSize;
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
