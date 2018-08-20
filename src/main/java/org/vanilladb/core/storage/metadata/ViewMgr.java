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
package org.vanilladb.core.storage.metadata;

import java.util.Collection;
import java.util.LinkedList;

import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.metadata.TableMgr.MAX_NAME;

import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

class ViewMgr {
	/**
	 * Name of the view catalog.
	 */
	public static final String VCAT = "viewcat";

	/**
	 * A field name of the view catalog.
	 */
	public static final String VCAT_VNAME = "viewname", VCAT_VDEF = "viewdef";

	private static final int MAX_VIEWDEF;
	TableMgr tblMgr;

	static {
		MAX_VIEWDEF = CoreProperties.getLoader().getPropertyAsInteger(
				ViewMgr.class.getName() + ".MAX_VIEWDEF", 100);
	}

	public ViewMgr(boolean isNew, TableMgr tblMgr, Transaction tx) {
		this.tblMgr = tblMgr;
		if (isNew) {
			Schema sch = new Schema();
			sch.addField(VCAT_VNAME, VARCHAR(MAX_NAME));
			sch.addField(VCAT_VDEF, VARCHAR(MAX_VIEWDEF));
			tblMgr.createTable(VCAT, sch, tx);
		}
	}

	public void createView(String vName, String vDef, Transaction tx) {
		TableInfo ti = tblMgr.getTableInfo(VCAT, tx);
		RecordFile rf = ti.open(tx, true);
		rf.insert();
		rf.setVal(VCAT_VNAME, new VarcharConstant(vName));
		rf.setVal(VCAT_VDEF, new VarcharConstant(vDef));
		rf.close();
	}

	public void dropView(String vName, Transaction tx) {
		TableInfo ti = tblMgr.getTableInfo(VCAT, tx);
		RecordFile rf = ti.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			if (rf.getVal(VCAT_VNAME).equals(new VarcharConstant(vName)))
				rf.delete();
		}
		rf.close();
	}

	public String getViewDef(String vName, Transaction tx) {
		String result = null;
		TableInfo ti = tblMgr.getTableInfo(VCAT, tx);
		RecordFile rf = ti.open(tx, true);
		rf.beforeFirst();
		while (rf.next())
			if (((String) rf.getVal(VCAT_VNAME).asJavaVal()).equals(vName)) {
				result = (String) rf.getVal(VCAT_VDEF).asJavaVal();
				break;
			}
		rf.close();
		return result;
	}

	// XXX: This makes the storage engine depend on the query engine.
	// We may have to come out a better method.
	public Collection<String> getViewNamesByTable(String tblName, Transaction tx) {
		Collection<String> result = new LinkedList<String>();

		TableInfo ti = tblMgr.getTableInfo(VCAT, tx);
		RecordFile rf = ti.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			Parser parser = new Parser((String) rf.getVal(VCAT_VDEF).asJavaVal());
			if (parser.queryCommand().tables().contains(tblName))
				result.add((String) rf.getVal(VCAT_VNAME).asJavaVal());
		}
		rf.close();

		return result;
	}
}
