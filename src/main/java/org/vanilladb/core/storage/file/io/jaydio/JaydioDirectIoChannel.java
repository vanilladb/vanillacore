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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.storage.file.io.IoBuffer;
import org.vanilladb.core.storage.file.io.IoChannel;
import org.vanilladb.core.util.TransactionProfiler;

import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import net.smacke.jaydio.channel.BufferedChannel;
import net.smacke.jaydio.channel.DirectIoByteChannel;

public class JaydioDirectIoChannel implements IoChannel {

	private BufferedChannel<AlignedDirectByteBuffer> fileChannel;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	// Optimization: store the size of each table
	private long fileSize;

	public JaydioDirectIoChannel(File file) throws IOException {
		fileChannel = DirectIoByteChannel.getChannel(file, false);
		fileSize = fileChannel.size();
	}

	@Override
	public int read(IoBuffer buffer, long position) throws IOException {
		// profiler
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		int stage = TransactionProfiler.getStageIndicator();
		String op = TransactionProfiler.getOperationIndicator();
		
		profiler.startComponentProfiler(stage + op + "-JaydioDirectioChannel.read readLock");
		lock.readLock().lock();
		profiler.stopComponentProfiler(stage + op + "-JaydioDirectioChannel.read readLock");
		
		try {
			JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
			return fileChannel.read(jaydioBuffer.getAlignedDirectByteBuffer(), position);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public int write(IoBuffer buffer, long position) throws IOException {
		// profiler
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		int stage = TransactionProfiler.getStageIndicator();
		String op = TransactionProfiler.getOperationIndicator();
		
		profiler.startComponentProfiler(stage + op + "-JaydioDirectioChannel.write writeLock");
		lock.writeLock().lock();
		profiler.stopComponentProfiler(stage + op + "-JaydioDirectioChannel.write writeLock");
		
		try {
			JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
			int writeSize = fileChannel.write(jaydioBuffer.getAlignedDirectByteBuffer(), position);

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
		// profiler
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		int stage = TransactionProfiler.getStageIndicator();
		String op = TransactionProfiler.getOperationIndicator();
		
		profiler.startComponentProfiler(stage + op + "-JaydioDirectioChannel.append writeLock");
		lock.writeLock().lock();
		profiler.stopComponentProfiler(stage + op + "-JaydioDirectioChannel.append writeLock");
		
		try {
			JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
			int appendSize = fileChannel.write(jaydioBuffer.getAlignedDirectByteBuffer(), fileSize);
			fileSize += appendSize;
			return fileSize;
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public long size() throws IOException {
		// profiler
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		int stage = TransactionProfiler.getStageIndicator();
		String op = TransactionProfiler.getOperationIndicator();
		
		profiler.startComponentProfiler(stage + op + "-JaydioDirectioChannel.size readLock");
		lock.readLock().lock();
		profiler.stopComponentProfiler(stage + op + "-JaydioDirectioChannel.size readLock");
		
		try {
			return fileSize;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public void close() throws IOException {
		// profiler
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		int stage = TransactionProfiler.getStageIndicator();
		String op = TransactionProfiler.getOperationIndicator();
		
		profiler.startComponentProfiler(stage + op + "-JaydioDirectioChannel.close writeLock");
		lock.writeLock().lock();
		profiler.stopComponentProfiler(stage + op + "-JaydioDirectioChannel.size writeLock");
		
		try {
			fileChannel.close();
		} finally {
			lock.writeLock().unlock();
		}
	}
}
