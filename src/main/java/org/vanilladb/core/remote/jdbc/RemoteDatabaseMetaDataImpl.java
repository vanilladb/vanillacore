package org.vanilladb.core.remote.jdbc;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteDatabaseMetaDataImpl extends UnicastRemoteObject implements
        RemoteDatabaseMetaData {
    private RemoteConnectionImpl rconn;
    
    public RemoteDatabaseMetaDataImpl(RemoteConnectionImpl rconn) throws RemoteException {
        this.rconn = rconn;
    }
    
    @Override
    public RemoteResultSetImpl getTablesInfo(String qry) throws RemoteException {
        try {
            System.out.println("查询语句：" + qry);
            Transaction tx = rconn.getTransaction();
            Plan pln = VanillaDb.newPlanner().executeShowPlan(qry,tx);
            return new RemoteResultSetImpl(pln, rconn);
        } catch (RuntimeException e) {
            rconn.rollback();
            throw e;
        }
    }
}
