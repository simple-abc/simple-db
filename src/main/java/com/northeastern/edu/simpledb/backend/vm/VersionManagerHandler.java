package com.northeastern.edu.simpledb.backend.vm;

import com.northeastern.edu.simpledb.backend.dm.DataManger;
import com.northeastern.edu.simpledb.backend.tm.TransactionManager;

public interface VersionManagerHandler {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    static VersionManagerHandler newVersionManager(TransactionManager tm, DataManger dm) {
        return new VersionManager(tm, dm);
    }


}
