package org.vanilladb.core.remote.jdbc;

import com.sun.deploy.util.StringUtils;

import java.rmi.RemoteException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class JdbcDatabaseMetaData extends DatabaseMetaDataAdapter {
    private RemoteDatabaseMetaData rdmd;
    
    public JdbcDatabaseMetaData(RemoteDatabaseMetaData rdmd) {
        this.rdmd = rdmd;
    }
    
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        RemoteResultSet rrs = null;
        try {
            System.out.println("catalog=" + catalog + " schemaPattern=" + schemaPattern + " tableNamePattern=" + tableNamePattern + " types=" + Arrays.toString(types));
            String qry = getExecQueryEvent(catalog, schemaPattern);
            rrs = rdmd.getTablesInfo(qry);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        return new JdbcResultSet(rrs);
    }
    
    private String getExecQueryEvent(String catalog, String schemaPattern) {
        String qry;
        if (catalog == null) {
            qry = "show tables";
        } else {
            qry = "desc " + catalog;
        }
        return qry;
    }
    
}
