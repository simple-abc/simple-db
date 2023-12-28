package com.northeastern.edu.simpledb.backend.dm.dataItem;

import com.northeastern.edu.simpledb.backend.common.SubArray;
import com.northeastern.edu.simpledb.backend.dm.DataManger;
import com.northeastern.edu.simpledb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataItem extends AbstractDataItem {

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    static final byte VALID_STATE = 0b0;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManger dm;
    private long uid;
    private Page page;

    public DataItem(SubArray raw, byte[] oldRaw, Page page, long uid, DataManger dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.page = page;
    }

    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == VALID_STATE;
    }

    /**
     * The array returned by this method is data shared. Since
     * the array copy in Java will be allocated to a new address,
     * SubArray is used for data sharing between threads.
     */
    @Override
    public SubArray data() {
        return new SubArray(raw.raw,raw.start + OF_DATA, raw.end);
    }

    /**
     *  When the upper module tries to modify DataItem, it needs
     *  to follow a certain process: before modification, the before()
     *  method needs to be called. When you want to undo the modification,
     *  the unBefore() method is called. After the modification is
     *  completed, the after() method is called. The entire process
     *  is mainly to save the previous phase data and log it in time.
     *  DM will ensure that modifications to DataItem are atomic.
     */

    // save the previous phase data
    @Override
    public void before() {
        wLock.lock();
        page.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length); // latestRaw becomes oldRaw
    }

    // undo the previous action
    @Override
    public void unBefore() {
        wLock.lock();
        page.setDirty(true);
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length); // oldRaw overwrite latestRaw
    }

    // log in time
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    // after using data item, release it from DataManager
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
