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

import org.vanilladb.core.query.algebra.*;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;

import java.util.*;

public class BasicShowPlanner implements ShowPlanner {
    
    @Override
    public Plan showTables(String qry, Transaction tx) {
        Map<String, String> tableMsgMap = new HashMap<>();
        Map<String, Schema> schemaMap = new HashMap<>();
        Map<String, TableInfo> tableInfoMap = VanillaDb.catalogMgr().showTables();
        Set<String> tables = tableInfoMap.keySet();
        
        ShowScan showScan = new ShowScan("", tableMsgMap, null);
        ShowPlan showPlan = null;
        for (String tblname : tables) {
            TableInfo tableInfo = tableInfoMap.get(tblname);
            Schema schema = tableInfo.schema();
            schemaMap.put(tblname, schema);
            tableMsgMap = new HashMap<>();
            tableMsgMap.put("table_name", tblname);
            tableMsgMap.put("table_type", "表的类型-待实现");
            tableMsgMap.put("table_remarks", "表的描述-待实现");
            showScan = new ShowScan(tblname, tableMsgMap, showScan);
            showPlan = new ShowPlan(tblname, tableInfo, showScan);
        }
        return showPlan;
    }
    
    @Override
    public Plan descTable(String qry, Transaction tx) {
        String[] str = qry.split("\\s+");
        String tableName = str[1].toLowerCase();
        Map<String, TableInfo> tableInfoMap = VanillaDb.catalogMgr().showTables();
        TableInfo tableInfo = tableInfoMap.get(tableName);
        Map<String, Type> fields = new HashMap<String, Type>();
        Schema schema = tableInfo.schema();
        SortedSet<String> set = schema.fields();
        for (String name : set) {
            Type type = schema.type(name);
            fields.put(name, type);
        }
        DescScan tableScan = new DescScan(tableInfo, tx, fields);
        return new ShowPlan(tableName, tableInfo, tableScan);
    }
}
