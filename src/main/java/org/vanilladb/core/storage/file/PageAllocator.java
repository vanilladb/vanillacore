package org.vanilladb.core.storage.file;

import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;

import org.vanilladb.core.storage.file.io.IoChannel;

public class PageAllocator {
	private final String fileName;
	private IoChannel fileChannel;
	private AtomicInteger curFileSize = new AtomicInteger();
	
	PageAllocator(String fileName, IoChannel fileChannel) throws IOException {
		this.fileName = fileName;
		this.fileChannel = fileChannel;
		this.curFileSize.set((int) (fileChannel.size()/Page.BLOCK_SIZE)); // throws IOException
	}

	BlockId getPage() {
		return new BlockId(fileName, curFileSize.getAndIncrement());
	}
	
	IoChannel getFileChannel() {
		return fileChannel;
	}
	
	int getFileSize() {
		return curFileSize.get();
	}
}
