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
package org.vanilladb.core.server;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.io.File;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.statistics.StatMgr;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class ServerInit {
	private static Logger logger = Logger.getLogger(ServerInit.class.getName());

	public static int courseMax = 300, studentMax = 900, deptMax = 40,
			sectMax = 1200, enrollMax = 2000;
	public static final String DB_MAIN_DIR = "vanilladb_testdbs";
	
	// Flags
	private static final BlockId FLAG_DATA_BLOCK = new BlockId("testing_flags", 0);
	private static final int LOADED_FLAG_POS = 0;
	private static final Type LOADED_FLAG_TYPE = Type.INTEGER;
	private static final Constant DATA_LOADED_VALUE = new IntegerConstant(1);

	/**
	 * Initiates {@link VanillaDb}.
	 * 
	 * <p>
	 * Note that for each test class, members (e.g., static fields,
	 * constructors, etc) of all VanillaDb classes should be accessed after
	 * calling this method to ensure the proper class loading.
	 * </p>
	 */
	public static void init(Class<?> testClass) {
		String testClassName = testClass.getName();
		String dbName = DB_MAIN_DIR + "/" + testClassName;
		
		// Creates the main directory if it was not created before
		File dbPath = new File(FileMgr.DB_FILES_DIR, DB_MAIN_DIR);
		if (!dbPath.exists())
			dbPath.mkdir();
		
		// Deletes the existing database
		deleteDB(dbName);
		
		// Initializes a fresh database
		VanillaDb.init(dbName);
	}

	/**
	 * Set up a database for testing.
	 */
	public static void loadTestbed() {
		if (!checkIfTestbedLoaded()) {
			if (logger.isLoggable(Level.INFO))
				logger.info("loading data");
			CatalogMgr md = VanillaDb.catalogMgr();
			StatMgr stat = VanillaDb.statMgr();
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);

			// create and populate the course table
			Schema sch = new Schema();
			sch.addField("cid", INTEGER);
			sch.addField("title", VARCHAR(20));
			sch.addField("deptid", INTEGER);
			md.createTable("course", sch, tx);
			TableInfo ti = md.getTableInfo("course", tx);

			RecordFile rf = ti.open(tx, true);
			rf.beforeFirst();
			while (rf.next())
				rf.delete();
			rf.close();

			rf = ti.open(tx, true);
			for (int id = 0; id < courseMax; id++) {
				rf.insert();
				IntegerConstant cid = new IntegerConstant(id);
				rf.setVal("cid", cid);
				rf.setVal("title", new VarcharConstant("course" + id));
				rf.setVal("deptid", new IntegerConstant(id % deptMax));
			}
			rf.close();
			// refresh the statistical information after populating this table
			stat.getTableStatInfo(ti, tx);

			// create and populate the student table
			sch = new Schema();
			sch.addField("sid", INTEGER);
			sch.addField("sname", VARCHAR(10));
			sch.addField("majorid", INTEGER);
			sch.addField("gradyear", INTEGER);
			md.createTable("student", sch, tx);
			ti = md.getTableInfo("student", tx);

			rf = ti.open(tx, true);
			rf.beforeFirst();
			while (rf.next())
				rf.delete();
			rf.close();

			rf = ti.open(tx, true);
			for (int id = 0; id < studentMax; id++) {
				rf.insert();
				IntegerConstant sid = new IntegerConstant(id);
				rf.setVal("sid", sid);
				rf.setVal("sname", new VarcharConstant("student" + id));
				rf.setVal("majorid", new IntegerConstant(id % deptMax));
				rf.setVal("gradyear", new IntegerConstant((id % 50) + 1960));
			}
			rf.close();
			// refresh the statistical information after populating this table
			stat.getTableStatInfo(ti, tx);

			// create and populate the dept table
			sch = new Schema();
			sch.addField("did", INTEGER);
			sch.addField("dname", VARCHAR(8));
			md.createTable("dept", sch, tx);
			ti = md.getTableInfo("dept", tx);

			rf = ti.open(tx, true);
			rf.beforeFirst();
			while (rf.next())
				rf.delete();
			rf.close();

			rf = ti.open(tx, true);
			for (int id = 0; id < deptMax; id++) {
				rf.insert();
				IntegerConstant did = new IntegerConstant(id);
				rf.setVal("did", did);
				rf.setVal("dname", new VarcharConstant("dept" + id));
			}
			rf.close();
			// refresh the statistical information after populating this table
			stat.getTableStatInfo(ti, tx);

			// create and populate the section table
			sch = new Schema();
			sch.addField("sectid", INTEGER);
			sch.addField("prof", VARCHAR(8));
			sch.addField("courseid", INTEGER);
			sch.addField("yearoffered", INTEGER);
			md.createTable("section", sch, tx);
			ti = md.getTableInfo("section", tx);

			rf = ti.open(tx, true);
			rf.beforeFirst();
			while (rf.next())
				rf.delete();
			rf.close();

			rf = ti.open(tx, true);
			for (int id = 0; id < sectMax; id++) {
				rf.insert();
				IntegerConstant sectid = new IntegerConstant(id);
				rf.setVal("sectid", sectid);
				int profnum = id % 20;
				rf.setVal("prof", new VarcharConstant("prof" + profnum));
				rf.setVal("courseid", new IntegerConstant(id % courseMax));
				rf.setVal("yearoffered", new IntegerConstant((id % 50) + 1960));
			}
			rf.close();
			// refresh the statistical information after populating this table
			stat.getTableStatInfo(ti, tx);

			// create and populate the enroll table
			sch = new Schema();
			sch.addField("eid", INTEGER);
			sch.addField("grade", VARCHAR(2));
			sch.addField("studentid", INTEGER);
			sch.addField("sectionid", INTEGER);
			md.createTable("enroll", sch, tx);
			ti = md.getTableInfo("enroll", tx);

			rf = ti.open(tx, true);
			rf.beforeFirst();
			while (rf.next())
				rf.delete();
			rf.close();

			rf = ti.open(tx, true);
			String[] grades = new String[] { "A", "B", "C", "D", "F" };
			for (int id = 0; id < enrollMax; id++) {
				rf.insert();
				IntegerConstant eid = new IntegerConstant(id);
				rf.setVal("eid", eid);
				rf.setVal("grade", new VarcharConstant(grades[id % 5]));
				rf.setVal("studentid", new IntegerConstant(id % studentMax));
				rf.setVal("sectionid", new IntegerConstant(id % sectMax));
			}
			rf.close();
			// refresh the statistical information after populating this table
			stat.getTableStatInfo(ti, tx);

			tx.commit();

			// add a checkpoint record to limit rollback
			tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			RecoveryMgr.initializeSystem(tx);
			tx.commit();
			
			// Set the flag indicating that the data is loaded
			setFlagAsLoaded();
		}
	}

	private static void deleteDB(String dbName) {
		File dbPath = new File(FileMgr.DB_FILES_DIR, dbName);
		if (dbPath.exists()) {
			File[] files = dbPath.listFiles();
			for (File file : files) {
				if (!file.delete())
					throw new RuntimeException("cannot delete the file: " + file);
			}
			
			if (!dbPath.delete())
				throw new RuntimeException("cannot delete the directory: " + dbPath);
		}
	}
	
	private static void setFlagAsLoaded() {
		Page page = new Page();
		page.setVal(LOADED_FLAG_POS, DATA_LOADED_VALUE);
		page.write(FLAG_DATA_BLOCK);
	}
	
	private static boolean checkIfTestbedLoaded() {
		Page page = new Page();
		page.read(FLAG_DATA_BLOCK);
		Constant isLoaded = page.getVal(LOADED_FLAG_POS, LOADED_FLAG_TYPE);
		
		if (isLoaded.equals(DATA_LOADED_VALUE))
			return true;
		return false;
	}
}
