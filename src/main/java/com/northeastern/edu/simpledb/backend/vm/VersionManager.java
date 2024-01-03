package com.northeastern.edu.simpledb.backend.vm;

import com.northeastern.edu.simpledb.backend.common.AbstractCache;
import com.northeastern.edu.simpledb.backend.dm.DataManger;
import com.northeastern.edu.simpledb.backend.tm.TransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.northeastern.edu.simpledb.backend.tm.TransactionManager.SUPER_XID;
import static com.northeastern.edu.simpledb.common.Error.NullEntryException;

public class VersionManager extends AbstractCache<Entry> implements VersionManagerHandler {

    TransactionManager tm;
    DataManger dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManager(TransactionManager tm, DataManger dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        activeTransaction = new ConcurrentHashMap<>();
        activeTransaction.put(SUPER_XID, Transaction.newTransaction(SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        Transaction transaction = activeTransaction.get(xid);
        if (transaction.err != null) throw transaction.err;

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == NullEntryException) {
                return null;
            } else throw e;
        }

        try {
            if (Visibility.isVisible(tm, transaction, entry)) {
                return entry.data();
            } else return null;
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        Transaction transaction = activeTransaction.get(xid);

        if (transaction.err != null) throw transaction.err;

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        return false;
    }

    @Override
    public long begin(int level) {
        return 0;
    }

    @Override
    public void commit(long xid) throws Exception {

    }

    @Override
    public void abort(long xid) {

    }

    @Override
    protected Entry getForCache(long key) throws Exception {
        return null;
    }

    @Override
    protected void releaseForCache(Entry obj) {

    }
}
