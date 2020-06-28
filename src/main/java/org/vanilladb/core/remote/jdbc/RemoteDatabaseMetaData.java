package org.vanilladb.core.remote.jdbc;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteDatabaseMetaData extends Remote {
    
    RemoteResultSet getTablesInfo(String qry) throws RemoteException;
}
