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
package org.vanilladb.core.query.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.DropIndexData;
import org.vanilladb.core.query.parse.DropTableData;
import org.vanilladb.core.query.parse.DropViewData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.aggfn.AggregationFn;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.TableMgr;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The verifier which examines the semantic of input query and update
 * statements.
 * 
 */
public class Verifier {

	public static void verifyQueryData(QueryData data, Transaction tx) {
		List<Schema> schs = new ArrayList<Schema>(data.tables().size());
		List<QueryData> views = new ArrayList<QueryData>(data.tables().size());

		// examine the table name
		for (String tblName : data.tables()) {
			String viewdef = VanillaDb.catalogMgr().getViewDef(tblName, tx);
			if (viewdef == null) {
				TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
				if (ti == null)
					throw new BadSemanticException("table " + tblName
							+ " does not exist");
				schs.add(ti.schema());
			} else {
				Parser parser = new Parser(viewdef);
				views.add(parser.queryCommand());
			}
		}

		// examine the projecting field name
		for (String fldName : data.projectFields()) {
			boolean isValid = verifyField(schs, views, fldName);
			if (!isValid && data.aggregationFn() != null)
				for (AggregationFn aggFn : data.aggregationFn())
					if (fldName.compareTo(aggFn.fieldName()) == 0) {
						isValid = true;
						break;
					}
			if (!isValid)
				throw new BadSemanticException("field " + fldName
						+ " does not exist");
		}

		// examine the aggregation field name
		if (data.aggregationFn() != null)
			for (AggregationFn aggFn : data.aggregationFn()) {
				String aggFld = aggFn.argumentFieldName();
				if (!verifyField(schs, views, aggFld))
					throw new BadSemanticException("field " + aggFld
							+ " does not exist");
			}

		// examine the grouping field name
		if (data.groupFields() != null)
			for (String groupByFld : data.groupFields()) {
				if (!verifyField(schs, views, groupByFld))
					throw new BadSemanticException("field " + groupByFld
							+ " does not exist");
			}

		// Examine the sorting field name
		if (data.sortFields() != null)
			for (String sortFld : data.sortFields()) {
				boolean isValid = verifyField(schs, views, sortFld);
				
				// aggregation field may appear after order by
				// example: select count(fld1), fld2 from table group by fld2 order by count(fld1);
				// we need the following checks to make count(fld1) valid
				if (!isValid && data.aggregationFn() != null)
					for (AggregationFn aggFn : data.aggregationFn())
						if (sortFld.compareTo(aggFn.fieldName()) == 0) {
							isValid = true;
							break;
						}
				if (!isValid)
					throw new BadSemanticException("field " + sortFld
							+ " does not exist");
			}
	}

	public static void verifyInsertData(InsertData data, Transaction tx) {
		// examine table name
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(data.tableName(), tx);
		if (ti == null)
			throw new BadSemanticException("table " + data.tableName()
					+ " does not exist");

		Schema sch = ti.schema();
		List<String> fields = data.fields();
		List<Constant> vals = data.vals();

		// examine whether values have the same size with fields
		if (fields.size() != vals.size())
			throw new BadSemanticException("table " + data.tableName()
					+ " :#fields and #values does not match");

		// every field defined in the schema should have an insert value
		if (fields.size() != ti.schema().fields().size())
			throw new BadSemanticException(
					"table "
							+ data.tableName()
							+ " :#fields going to be inserted is not the same as the schema definition");

		// examine the fields existence and type
		for (int i = 0; i < fields.size(); i++) {
			String field = fields.get(i);
			Constant val = vals.get(i);
			
			// check field existence
			if (!sch.hasField(field))
				throw new BadSemanticException("field " + field
						+ " does not exist");
			// check whether field match value type
			if (!matchFieldAndConstant(sch, field, val))
				throw new BadSemanticException("field " + field
						+ " doesn't match corresponding value in type");
		}
	}

	public static void verifyModifyData(ModifyData data, Transaction tx) {
		// examine Table name
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(data.tableName(), tx);
		if (ti == null)
			throw new BadSemanticException("table " + data.tableName()
					+ " does not exist");

		// examine the fields existence and type
		Schema sch = ti.schema();
		for (String field : data.targetFields()) {
			// check field existence
			if (!sch.hasField(field))
				throw new BadSemanticException("field " + field
						+ " does not exist");
			// check whether field match new value type
			if (!data.newValue(field).isApplicableTo(sch))
				throw new BadSemanticException("new value of field " + field
						+ " does not exist");
		}
	}

	public static void verifyDeleteData(DeleteData data, Transaction tx) {
		// examine table name
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(data.tableName(), tx);
		if (ti == null)
			throw new BadSemanticException("table " + data.tableName()
					+ " does not exist");
	}

	public static void verifyCreateTableData(CreateTableData data,
			Transaction tx) {
		// examine table name
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(data.tableName(), tx);
		if (ti != null)
			throw new BadSemanticException("table " + data.tableName()
					+ " already exist");
		if (data.tableName().length() > TableMgr.MAX_NAME)
			throw new BadSemanticException("the length of table name '"
					+ data.tableName()
					+ "' is too long; see the properties file ");
		Set<String> flds = data.newSchema().fields();
		for (String fld : flds)
			if (fld.length() > TableMgr.MAX_NAME)
				throw new BadSemanticException("the length of field name '"
						+ fld + "' is too long; see the properties file ");
	}

	public static void verifyDropTableData(DropTableData data, Transaction tx) {
		// examine table name
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(data.tableName(), tx);
		if (ti == null)
			throw new BadSemanticException("table " + data.tableName()
					+ " does not exist");
	}

	public static void verifyCreateIndexData(CreateIndexData data,
			Transaction tx) {
		// examine table name
		String tableName = data.tableName();
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tableName, tx);
		if (ti == null)
			throw new BadSemanticException("table " + tableName
					+ " does not exist");
		
		// examine if column exist
		Schema sch = ti.schema();
		List<String> fieldNames = data.fieldNames();
		for (String fieldName : fieldNames) {
			if (!sch.hasField(fieldName))
				throw new BadSemanticException("field " + fieldName
						+ " does not exist in table " + tableName);
		}
		
		// examine the index name
		if (VanillaDb.catalogMgr().getIndexInfoByName(data.indexName(), tx) != null)
			throw new BadSemanticException("index " + data.indexName()
					+ " has already existed");
	}

	public static void verifyDropIndexData(DropIndexData data, Transaction tx) {
		// examine index name
		if (VanillaDb.catalogMgr().getIndexInfoByName(data.indexName(), tx) == null)
			throw new BadSemanticException("index " + data.indexName()
					+ " does not exist");
	}

	public static void verifyCreateViewData(CreateViewData data, Transaction tx) {
		// examine view name
		if (VanillaDb.catalogMgr().getViewDef(data.viewName(), tx) != null)
			throw new BadSemanticException("view name duplicated");

		// examine query data
		verifyQueryData(data.viewDefData(), tx);
	}

	public static void verifyDropViewData(DropViewData data, Transaction tx) {
		// examine view name
		if (VanillaDb.catalogMgr().getViewDef(data.viewName(), tx) == null)
			throw new BadSemanticException("view " + data.viewName()
					+ " does not exist");
	}

	private static boolean matchFieldAndConstant(Schema sch, String field,
			Constant val) {
		Type type = sch.type(field);
		if (type.isNumeric() && val instanceof VarcharConstant)
			return false;
		else if (!type.isNumeric() && !(val instanceof VarcharConstant))
			return false;
		else
			return true;
	}

	private static boolean verifyField(List<Schema> schs,
			List<QueryData> views, String fld) {
		for (Schema s : schs) {
			if (s.hasField(fld)) {
				return true;
			}
		}

		for (QueryData queryData : views) {
			if (queryData.projectFields().contains(fld)) {
				return true;
			}
		}
		
		return false;
	}
}
