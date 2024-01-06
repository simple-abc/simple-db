package com.northeastern.edu.simpledb.backend.vm;

import com.northeastern.edu.simpledb.backend.common.AbstractCache;
import com.northeastern.edu.simpledb.backend.dm.DataManger;
import com.northeastern.edu.simpledb.backend.tm.TransactionManager;
import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.common.Error;

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
        Transaction transaction = activeTransaction.get(xid);
        if (transaction.err != null) {
            throw transaction.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == NullEntryException) {
                return false;
            } else throw e;
        }

        try {
            if (!Visibility.isVisible(tm, transaction, entry)) {
                return false;
            }

            Lock l = null;
            try {
                l = lt.add(xid, uid); // try to delete it
            } catch (Exception e) {
                transaction.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                transaction.autoAborted = true;
                throw transaction.err;
            }

            if (l != null) {
                l.lock();
                l.unlock();
            }

            if (entry.getXmax() == xid) { // has been deleted
                return false;
            }

            if (Visibility.isVersionSkip(tm, transaction, entry)) { // version skip, do rollback
                transaction.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                transaction.autoAborted = true;
                throw transaction.err;
            }

            entry.setXmax(xid); // signal entry as deleted
            return true;

        } finally {
            entry.release();
        }
    }

    /**
     * abort delete when exception happens
     * @param autoAborted: if it happens by accident, autoAborted is true
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        if (!autoAborted) activeTransaction.remove(xid);
        lock.unlock();

        if (!transaction.autoAborted) {
            lt.remove(xid);
            tm.abort(xid);
        }
    }

    /**
     * start a transaction
     * when new a transaction, do a snapshot of active transaction
     * for the validation of visibility
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction transaction = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, transaction);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        Transaction transaction = activeTransaction.get(xid);

        try {
            if (transaction.err != null) throw transaction.err;
        } catch (NullPointerException e) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(e);
        }

        activeTransaction.remove(xid);

        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null) throw NullEntryException;
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }
}
