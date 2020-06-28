package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;

import java.util.Map;

public class DescScan implements Scan {
    private TableInfo ti;
    private Schema schema;
    private Map<String, Type> fields;
    
    public DescScan(TableInfo ti, Transaction tx, Map<String, Type> fields) {
        this.ti = ti;
        this.schema = ti.schema();
        this.fields = fields;
    }
    
    @Override
    public void beforeFirst() {
    
    }
    
    @Override
    public boolean next() {
        return fields.size() > 0;
    }
    
    @Override
    public void close() {
    
    }
    
    @Override
    public boolean hasField(String fldName) {
        return schema.hasField(fldName);
    }
    
    @Override
    public Constant getVal(String fldName) {
        Type type = fields.get(fldName);
        String str = type.toString();
        return new VarcharConstant(str);
    }
}
