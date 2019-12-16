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
package org.vanilladb.core.storage.metadata.index;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.metadata.TableMgr.MAX_NAME;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.TableMgr;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The index manager. The index manager has similar functionality to the table
 * manager.
 */
public class IndexMgr {
	/**
	 * The name of the index catalog.
	 */
	public static final String ICAT = "idxcat";

	/**
	 * The field names of the index catalog.
	 */
	public static final String ICAT_IDXNAME = "idxname",
			ICAT_TBLNAME = "tblname", ICAT_IDXTYPE = "idxtype";
	
	/**
	 * The name of the key catalog.
	 */
	public static final String KCAT = "idxkeycat";
	
	/**
	 * The field names of the key catalog.
	 */
	public static final String KCAT_IDXNAME = "idxname",
			KCAT_KEYNAME = "keyname";

	private TableInfo idxTi, keyTi;

	// Optimization: Materialize the index information
	// Index Name -> IndexInfo
	private Map<String, IndexInfo> iiMapByIdxNames;
	// Table Name -> (Field Name -> List of IndexInfos which uses the field)
	private Map<String, Map<String, List<IndexInfo>>> iiMapByTblAndFlds;
	// A set that indicates the tables whose IndexInfos are all loaded in the memory.
	private Set<String> loadedTables;

	/**
	 * Creates the index manager. This constructor is called during system
	 * startup. If the database is new, then the <em>idxcat</em> table is
	 * created.
	 * 
	 * @param isNew
	 *            indicates whether this is a new database
	 * @param tblMgr
	 *            the instance of the table manager, which is used to initialize catalog files
	 * @param tx
	 *            the system startup transaction
	 */
	public IndexMgr(boolean isNew, TableMgr tblMgr, Transaction tx) {
		if (isNew) {
			Schema sch = new Schema();
			sch.addField(ICAT_IDXNAME, VARCHAR(MAX_NAME));
			sch.addField(ICAT_TBLNAME, VARCHAR(MAX_NAME));
			sch.addField(ICAT_IDXTYPE, INTEGER);
			tblMgr.createTable(ICAT, sch, tx);

			sch = new Schema();
			sch.addField(KCAT_IDXNAME, VARCHAR(MAX_NAME));
			sch.addField(KCAT_KEYNAME, VARCHAR(MAX_NAME));
			tblMgr.createTable(KCAT, sch, tx);
		}
		
		idxTi = tblMgr.getTableInfo(ICAT, tx); 
		if (idxTi == null) 
			throw new RuntimeException("cannot find the catalog file for indices"); 
		keyTi = tblMgr.getTableInfo(KCAT, tx); 
		if (keyTi == null) 
			throw new RuntimeException("cannot find the catalog file for the keys of indices"); 
		
		/*
		 * Optimization: store the ii. WARNING: if allowing run-time index
		 * schema modification, this opt should be aware of the changing.
		 */
		iiMapByIdxNames = new ConcurrentHashMap<String, IndexInfo>();
		iiMapByTblAndFlds = new ConcurrentHashMap<String, Map<String, List<IndexInfo>>>();
		loadedTables = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	}

	/**
	 * Creates an index of the specified type for the specified field. A unique
	 * ID is assigned to this index, and its information is stored in the idxcat
	 * table.
	 * 
	 * @param idxName
	 *            the name of the index
	 * @param tblName
	 *            the name of the indexed table
	 * @param fldNames
	 *            the name of the indexed field
	 * @param idxType
	 *            the index type of the indexed field
	 * @param tx
	 *            the calling transaction
	 */
	public void createIndex(String idxName, String tblName, List<String> fldNames,
			IndexType idxType, Transaction tx) {
		
		// Add the index infos to the index catalog
		RecordFile rf = idxTi.open(tx, true);
		rf.insert();
		rf.setVal(ICAT_IDXNAME, new VarcharConstant(idxName));
		rf.setVal(ICAT_TBLNAME, new VarcharConstant(tblName));
		rf.setVal(ICAT_IDXTYPE, new IntegerConstant(idxType.toInteger()));
		rf.close();
		
		// Add the field names to the key catalog
		rf = keyTi.open(tx, true);
		for (String fldName : fldNames) {
			rf.insert();
			rf.setVal(KCAT_IDXNAME, new VarcharConstant(idxName));
			rf.setVal(KCAT_KEYNAME, new VarcharConstant(fldName));
			rf.close();
		}
		
		updateCache(new IndexInfo(idxName, tblName, fldNames, idxType));
	}
	
	public Set<String> getIndexedFields(String tblName, Transaction tx) {
		// Check the cache
		if (!loadedTables.contains(tblName)) {
			readFromFile(tblName, tx);
		}
		
		// Fetch from the cache
		Map<String, List<IndexInfo>> iiMap = iiMapByTblAndFlds.get(tblName);
		if (iiMap == null)
			return Collections.emptySet(); // avoid object creation
		
		// Defense copy
		return new HashSet<String>(iiMap.keySet());
	}
	
	/**
	 * Returns a map containing the index info for all indexes on the specified
	 * table.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param fldName
	 *            the name of the search field
	 * @param tx
	 *            the context of executing transaction
	 * @return a map of IndexInfo objects, keyed by their field names
	 */
	public List<IndexInfo> getIndexInfo(String tblName, String fldName, Transaction tx) {
		// Check the cache
		if (!loadedTables.contains(tblName)) {
			readFromFile(tblName, tx);
		}
		
		// Fetch from the cache
		Map<String, List<IndexInfo>> iiMap = iiMapByTblAndFlds.get(tblName);
		if (iiMap == null)
			return Collections.emptyList(); // avoid object creation
		List<IndexInfo> iiList = iiMap.get(fldName);
		if (iiList == null)
			return Collections.emptyList(); // avoid object creation
		
		// Defense copy
		return new LinkedList<IndexInfo>(iiList);
	}

	/**
	 * Returns the requested index info object with the given index name.
	 * 
	 * @param idxName
	 *            the name of the index
	 * @param tx
	 *            the calling transaction
	 * @return an IndexInfo object
	 */
	public IndexInfo getIndexInfoByName(String idxName, Transaction tx) {
		// Fetch from the cache
		IndexInfo ii = iiMapByIdxNames.get(idxName);
		if (ii != null)
			return ii;
		
		// Read from the catalog files
		String tblName = null;
		List<String> fldNames = new LinkedList<String>();
		IndexType idxType = null;
		
		// Find the index in the index catalog
		RecordFile rf = idxTi.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			if (((String) rf.getVal(ICAT_IDXNAME).asJavaVal()).equals(idxName)) {
				tblName = (String) rf.getVal(ICAT_TBLNAME).asJavaVal();
				int idxtypeVal = (Integer) rf.getVal(ICAT_IDXTYPE).asJavaVal();
				idxType = IndexType.fromInteger(idxtypeVal);
				break;
			}
		}
		rf.close();
		
		if (tblName == null)
			return null;
		
		// Find the corresponding field names
		rf = keyTi.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			if (((String) rf.getVal(KCAT_IDXNAME).asJavaVal()).equals(idxName)) {
				fldNames.add((String) rf.getVal(KCAT_KEYNAME).asJavaVal());
			}
		}
		rf.close();
		
		// Materialize IndexInfos
		ii = new IndexInfo(idxName, tblName, fldNames, idxType);
		updateCache(ii);
		
		return ii;
	}

	/**
	 * Remove an index of the specified type for the specified field. A unique
	 * ID is assigned to this index, and its information is stored in the idxcat
	 * table.
	 * 
	 * @param idxName
	 *            the name of the index
	 * @param tx
	 *            the calling transaction
	 */
	public void dropIndex(String idxName, Transaction tx) {
		// Remove from the catalog files
		String tblName = null;
		List<String> fldNames = new LinkedList<String>();
		IndexType idxType = null;
		
		// Find the index in the index catalog
		RecordFile rf = idxTi.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			if (((String) rf.getVal(ICAT_IDXNAME).asJavaVal()).equals(idxName)) {
				tblName = (String) rf.getVal(ICAT_TBLNAME).asJavaVal();
				int idxtypeVal = (Integer) rf.getVal(ICAT_IDXTYPE).asJavaVal();
				idxType = IndexType.fromInteger(idxtypeVal);
				rf.delete();
				break;
			}
		}
		rf.close();
		
		if (tblName == null)
			return;
		
		// Find the corresponding field names
		rf = keyTi.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			if (((String) rf.getVal(KCAT_IDXNAME).asJavaVal()).equals(idxName)) {
				fldNames.add((String) rf.getVal(KCAT_KEYNAME).asJavaVal());
				rf.delete();
			}
		}
		rf.close();

		// update the cache
		removeFromCache(new IndexInfo(idxName, tblName, fldNames, idxType));
	}
	
	private void readFromFile(String tblName, Transaction tx) {
		// Read from the catalog files
		Map<String, IndexType> idxTypeMap = new HashMap<String, IndexType>();
		
		// Find all the indexes for the table
		RecordFile rf = idxTi.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			if (((String) rf.getVal(ICAT_TBLNAME).asJavaVal()).equals(tblName)) {
				String idxname = (String) rf.getVal(ICAT_IDXNAME).asJavaVal();
				int idxtype = (Integer) rf.getVal(ICAT_IDXTYPE).asJavaVal();
				idxTypeMap.put(idxname, IndexType.fromInteger(idxtype));
			}
		}
		rf.close();
		
		// Find the key names of the indexes
		Map<String, List<String>> fldNamesMap = new HashMap<String, List<String>>();
		Set<String> idxNames = idxTypeMap.keySet();
		rf = keyTi.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			String idxName = (String) rf.getVal(KCAT_IDXNAME).asJavaVal();
			if (idxNames.contains(idxName)) {
				String field = (String) rf.getVal(KCAT_KEYNAME).asJavaVal();
				
				List<String> fldNames = fldNamesMap.get(idxName);
				if (fldNames == null) {
					fldNames = new LinkedList<String>();
					fldNamesMap.put(idxName, fldNames);
				}
				fldNames.add(field);
			}
		}
		rf.close();
		
		// Materialize IndexInfos
		for (String idxName : idxNames) {
			IndexType idxType = idxTypeMap.get(idxName);
			List<String> fldNames = fldNamesMap.get(idxName);
			updateCache(new IndexInfo(idxName, tblName, fldNames, idxType));
		}
		loadedTables.add(tblName);
	}
	
	private void updateCache(IndexInfo ii) {
		if (!iiMapByIdxNames.containsKey(ii.indexName()))
			iiMapByIdxNames.put(ii.indexName(), ii);

		// Update iiMapByTblAndFlds
		Map<String, List<IndexInfo>> iiMapByFldNames = iiMapByTblAndFlds.get(ii.tableName());
		if (iiMapByFldNames == null) {
			iiMapByFldNames = new ConcurrentHashMap<String, List<IndexInfo>>();
			iiMapByTblAndFlds.put(ii.tableName(), iiMapByFldNames);
		}
		
		for (String fldName : ii.fieldNames()) {
			List<IndexInfo> iiList = iiMapByFldNames.get(fldName);
			if (iiList == null) {
				iiList = new CopyOnWriteArrayList<IndexInfo>();
				iiMapByFldNames.put(fldName, iiList);
			}
			if (!iiList.contains(ii))
				iiList.add(ii);
		}
	}
	
	private void removeFromCache(IndexInfo ii) {
		iiMapByIdxNames.remove(ii.indexName());

		// Update iiMapByTblAndFlds
		Map<String, List<IndexInfo>> iiMapByFldNames = iiMapByTblAndFlds.get(ii.tableName());
		if (iiMapByFldNames != null) {
			for (String fldName : ii.fieldNames()) {
				List<IndexInfo> iiList = iiMapByFldNames.get(fldName);
				if (iiList != null)
					iiList.remove(ii);
			}
		}
	}
}
