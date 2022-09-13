package org.vanilladb.core.storage.file.io;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.vanilladb.core.storage.file.Page;

/**
 * Through VirtualChannel, unnecessary I/O on append can be removed to improve performance
 * VirtualChannel makes use of the decorator pattern
 * 
 * @author wilbertharriman
 *
 */
public class VirtualChannel implements IoChannel{
	private final IoChannel fileChannel;
	private AtomicInteger curFileSize = new AtomicInteger();
	
	public VirtualChannel(IoChannel fileChannel) throws IOException {
		this.fileChannel = fileChannel;
		this.curFileSize.set((int) fileChannel.size() / Page.BLOCK_SIZE);
	}
	
	@Override
	public int read(IoBuffer buffer, long position) throws IOException {
		return fileChannel.read(buffer, position);
	}

	@Override
	public int write(IoBuffer buffer, long position) throws IOException {
		int writeSize = fileChannel.write(buffer, position);
		
		if (size() < fileChannel.size()) {
			curFileSize.set((int) fileChannel.size() / Page.BLOCK_SIZE);
		}
		
		return writeSize;
	}

	/**
	 * @return address of the buffer in the file
	 */
	@Override
	public long append(IoBuffer buffer) throws IOException {
		// Optimization: buffer is not written to disk 
		// Warning: Don't use append to write to file
		return curFileSize.getAndIncrement();
	}

	@Override
	public long size() throws IOException {
		return curFileSize.get() * Page.BLOCK_SIZE;
	}

	@Override
	public void close() throws IOException {
		fileChannel.close();
	}
	
}