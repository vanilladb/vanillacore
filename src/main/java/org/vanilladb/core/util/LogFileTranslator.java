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
package org.vanilladb.core.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.log.LogMgr;
import org.vanilladb.core.storage.tx.recovery.LogReader;

public class LogFileTranslator {
	private static Logger logger = Logger.getLogger(LogFileTranslator.class
			.getName());
	private static String LOG_FILE_BASE_DIR;
	private static String LOG_FILE_NAME;

	// Read property file
	static {
		LOG_FILE_BASE_DIR = CoreProperties.getLoader().getPropertyAsString(
				FileMgr.class.getName() + ".LOG_FILE_BASE_DIR",
				System.getProperty("user.home"));
		LOG_FILE_NAME = CoreProperties.getLoader().getPropertyAsString(
				LogMgr.class.getName() + ".LOG_FILE", "vanilladb.log");
	}

	public static void main(String[] args) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Initializing...");

		// check argument
		if (args.length < 1)
			System.out.println("Please enter the name of database directory.");

		// initialize basic components
		VanillaDb.initFileMgr(args[0]);

		translateLogFile(args[0], "plaintextLogFile.txt");

		if (logger.isLoggable(Level.INFO))
			logger.info("Translation completed");
	}

	private static void translateLogFile(String dirName, String outputFileName) {
		LogReader reader = new LogReader(LOG_FILE_NAME);
		File dir = new File(LOG_FILE_BASE_DIR);
		dir = new File(dir, dirName);

		File logFile = new File(dir, LOG_FILE_NAME);
		if (logger.isLoggable(Level.INFO))
			logger.info("Read log data from \"" + logFile.toString() + "\"");

		try {
			// Translate log file
			File outputFile = new File(dir, outputFileName);
			BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
			while (reader.nextRecord()) {
				out.write(reader.getLogString());
				out.newLine();
			}
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
