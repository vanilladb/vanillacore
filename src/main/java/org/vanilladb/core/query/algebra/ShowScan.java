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
package org.vanilladb.core.query.algebra;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

import java.util.HashMap;
import java.util.Map;

public class ShowScan implements Scan {
    private String tableName;
    private ShowScan subScan = null;
    private Map<String, String> tableMsgMap = new HashMap<>();
    
    public ShowScan(String tableName, Map<String, String> tableMsgMap, ShowScan subScan) {
        this.tableName = tableName;
        this.tableMsgMap = tableMsgMap;
        this.subScan = subScan;
    }
    
    
    @Override
    public void beforeFirst() {
        System.out.println("---beforeFirst--");
    }
    
    @Override
    public boolean next() {
        if (this.subScan.tableMsgMap.size() == 0) {
            return false;
        }
        return true;
    }
    
    @Override
    public void close() {
        System.out.println("---close--");
    }
    
    @Override
    public boolean hasField(String fldName) {
        return true;
    }
    
    @Override
    public Constant getVal(String fldName) {
        String value = this.tableMsgMap.get(fldName);
        if (this.tableMsgMap.size() == 0 && value == null) {
            tableName = subScan.tableName;
            tableMsgMap = subScan.tableMsgMap;
            subScan = subScan.subScan;
            value = this.tableMsgMap.get(fldName);
        }
        if (this.tableMsgMap.containsKey(fldName)) {
            this.tableMsgMap.remove(fldName);
        }
        return new VarcharConstant(value);
    }
    
}
