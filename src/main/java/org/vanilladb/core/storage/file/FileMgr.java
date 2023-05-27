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
package org.vanilladb.core.storage.file;

import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;
import static org.vanilladb.core.storage.log.LogMgr.DEFAULT_LOG_FILE;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.io.IoAllocator;
import org.vanilladb.core.storage.file.io.IoBuffer;
import org.vanilladb.core.storage.file.io.IoChannel;
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.util.TransactionProfiler;

/**
 * The VanillaDb file manager. The database system stores its data as files
 * within a specified directory. The file manager provides methods for reading
 * the contents of a file block to a Java byte buffer, writing the contents of a
 * byte buffer to a file block, and appending the contents of a byte buffer to
 * the end of a file. These methods are called exclusively by the class
 * {@link org.vanilladb.core.storage.file.Page Page}, and are thus
 * package-private. The class also contains two public methods: Method
 * {@link #isNew() isNew} is called during system initialization by
 * {@link VanillaDb#init}. Method {@link #size(String) size} is called by the
 * log manager and transaction manager to determine the end of the file.
 */

public class FileMgr {
	private static Logger logger = Logger.getLogger(FileMgr.class.getName());

	public static final String DB_FILES_DIR, LOG_FILES_DIR;
	// XXX: This should be deal with by an upper layer
	public static final String TMP_FILE_NAME_PREFIX = "_temp";

	private File dbDirectory, logDirectory;
	private boolean isNew;
	private Map<String, IoChannel> openFiles = new ConcurrentHashMap<String, IoChannel>();
	// Optimization: if files art not empty, cache them
	private ConcurrentHashMap<String, Boolean> fileNotEmptyCache;

	static {
		String dbDir = CoreProperties.getLoader().getPropertyAsString(FileMgr.class.getName() + ".DB_FILES_DIR",
				System.getProperty("user.home"));
		String logDir = CoreProperties.getLoader().getPropertyAsString(FileMgr.class.getName() + ".LOG_FILES_DIR",
				dbDir);
		String defaultDir = System.getProperty("user.home");

		// Check if these two directories exist
		if (!new File(dbDir).exists()) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("the database directory '" + dbDir + "' doesn't exist, use the default directory: "
						+ defaultDir);
			dbDir = defaultDir;
		}
		if (!new File(logDir).exists()) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("the log files directory '" + logDir
						+ "' doesn't exist, use the same directory as database files: " + dbDir);
			logDir = dbDir;
		}

		DB_FILES_DIR = dbDir;
		LOG_FILES_DIR = logDir;
	}

	private final Object[] anchors = new Object[1009];

	private Object prepareAnchor(Object o) {
		int code = o.hashCode() % anchors.length;
		if (code < 0)
			code += anchors.length;
		return anchors[code];
	}

	/**
	 * Creates a file manager for the specified database. The database will be
	 * stored in a folder of that name in the user's home directory. If the
	 * folder does not exist, then a folder containing an empty database is
	 * created automatically. Files for all temporary tables (i.e. tables
	 * beginning with "_temp") will be deleted during initializing.
	 * 
	 * @param dbName
	 *            the name of the directory that holds the database
	 */
	public FileMgr(String dbName) {
		dbDirectory = new File(DB_FILES_DIR, dbName);

		// log files can be stored in a different directory
		logDirectory = new File(LOG_FILES_DIR, dbName);
		isNew = !dbDirectory.exists();

		// check the existence of log folder
		if (!isNew && !logDirectory.exists())
			throw new RuntimeException("log file for the existed " + dbName + " is missing");

		// create the directory if the database is new
		if (isNew && (!dbDirectory.mkdir()))
			throw new RuntimeException("cannot create " + dbName);

		// remove any leftover temporary tables
		for (String filename : dbDirectory.list())
			if (filename.startsWith(TMP_FILE_NAME_PREFIX))
				new File(dbDirectory, filename).delete();

		if (logger.isLoggable(Level.INFO))
			logger.info("block size " + Page.BLOCK_SIZE);

		for (int i = 0; i < anchors.length; ++i)
			anchors[i] = new Object();
		
		fileNotEmptyCache = new ConcurrentHashMap<String, Boolean>();
	}

	/**
	 * Reads the contents of a disk block into a byte buffer.
	 * 
	 * @param blk
	 *            a block ID
	 * @param buffer
	 *            the byte buffer
	 */
	void read(BlockId blk, IoBuffer buffer) {
		try {
			IoChannel fileChannel = getFileChannel(blk.fileName());

			// clear the buffer
			buffer.clear();

			// read a block from file
			fileChannel.read(buffer, blk.number() * BLOCK_SIZE);
			
			// for controller
			TransactionProfiler.getLocalProfiler().incrementDiskIOCount();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("cannot read block " + blk);
		}
	}

	/**
	 * Writes the contents of a byte buffer into a disk block.
	 * 
	 * @param blk
	 *            a block ID
	 * @param buffer
	 *            the byte buffer
	 */
	void write(BlockId blk, IoBuffer buffer) {
		try {
			IoChannel fileChannel = getFileChannel(blk.fileName());

			// rewind the buffer
			buffer.rewind();

			// write the block to the file
			fileChannel.write(buffer, blk.number() * BLOCK_SIZE);
			
			// for controller
			TransactionProfiler.getLocalProfiler().incrementDiskIOCount();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("cannot write block" + blk);
		}
	}

	/**
	 * Appends the contents of a byte buffer to the end of the specified file.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param buffer
	 *            the byte buffer
	 * @return a block ID refers to the newly-created block.
	 */
	BlockId append(String fileName, IoBuffer buffer) {
		try {
			IoChannel fileChannel = getFileChannel(fileName);

			// Rewind the buffer for writing
			buffer.rewind();

			// Append the block to the file
			long newSize = fileChannel.append(buffer);

			// Return the new block id
			return new BlockId(fileName, newSize / BLOCK_SIZE - 1);

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Returns the number of blocks in the specified file.
	 * 
	 * @param fileName
	 *            the name of the file
	 * 
	 * @return the number of blocks in the file
	 */
	public long size(String fileName) {
		try {
			IoChannel fileChannel = getFileChannel(fileName);
			return fileChannel.size() / BLOCK_SIZE;
		} catch (IOException e) {
			throw new RuntimeException("cannot access " + fileName);
		}
	}
	
	/**
	 * Returns true if a file is empty, else false.<br><br>
	 * 
	 * <b>Side Effect:</b> These method has cache mechanism.
	 * If the file is no longer empty,
	 * isFileEmpty will always return false unless restarting the database. 
	 * 
	 * @param fileName
	 *            the name of the file
	 * 
	 * @return whether a file is empty or not
	 */
	public boolean isFileEmpty(String fileName) {
		boolean cacheMiss = !fileNotEmptyCache.containsKey(fileName);

		// get the file size again, if cache miss occurs or the file is empty.
		if (cacheMiss || !fileNotEmptyCache.get(fileName)) {
			fileNotEmptyCache.put(fileName, size(fileName) > 0);
		}
		
		return !fileNotEmptyCache.get(fileName);
	}
	
	/**
	 * Returns a boolean indicating whether the file manager had to create a new
	 * database directory.
	 * 
	 * @return true if the database is new
	 */
	public boolean isNew() {
		return isNew;
	}

	/**
	 * Returns the file channel for the specified filename. The file channel is
	 * stored in a map keyed on the filename. If the file is not open, then it
	 * is opened and the file channel is added to the map.
	 * 
	 * @param fileName
	 *            the specified filename
	 * 
	 * @return the file channel associated with the open file.
	 * @throws IOException
	 */
	private IoChannel getFileChannel(String fileName) throws IOException {
		synchronized (prepareAnchor(fileName)) {
			IoChannel fileChannel = openFiles.get(fileName);

			if (fileChannel == null) {
				File dbFile = fileName.equals(DEFAULT_LOG_FILE) ? new File(logDirectory, fileName)
						: new File(dbDirectory, fileName);
				fileChannel = IoAllocator.newIoChannel(dbFile);

				openFiles.put(fileName, fileChannel);
			}

			return fileChannel;
		}
	}

	/**
	 * Delete the specified file.
	 * 
	 * @param fileName
	 *            the name of the target file
	 */
	public void delete(String fileName) {
		try {
			synchronized (prepareAnchor(fileName)) {
				// Close file, if it was opened
				IoChannel fileChannel = openFiles.remove(fileName);
				if (fileChannel != null)
					fileChannel.close();

				// Delete the file
				boolean hasDeleted = new File(dbDirectory, fileName).delete();
				if (!hasDeleted && logger.isLoggable(Level.WARNING))
					logger.warning("cannot delete file: " + fileName);
			}
		} catch (IOException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("there is something wrong when deleting " + fileName);
			e.printStackTrace();
		}
	}
}