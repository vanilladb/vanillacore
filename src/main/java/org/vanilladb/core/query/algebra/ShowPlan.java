package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class ShowPlan implements Plan {
    private String tblname;
    private TableInfo tableInfo;
    private Scan scan;
    
    public ShowPlan(String tblname, TableInfo tableInfo, Scan scan) {
        this.tblname = tblname;
        this.scan = scan;
        this.tableInfo = tableInfo;
    }
    
    public ShowPlan(String tableName, TableInfo tableInfo) {
        this.tblname = tableName;
        this.tableInfo = tableInfo;
    }
    
    @Override
    public Scan open() {
        return scan;
    }
    
    @Override
    public long blocksAccessed() {
        return 0;
    }
    
    @Override
    public Schema schema() {
        return tableInfo.schema();
    }
    
    @Override
    public Histogram histogram() {
        return null;
    }
    
    @Override
    public long recordsOutput() {
        return 0;
    }
}
