package com.northeastern.edu.simpledb.backend.vm;

import com.google.common.primitives.Bytes;
import com.northeastern.edu.simpledb.backend.common.SubArray;
import com.northeastern.edu.simpledb.backend.dm.dataItem.DataItem;
import com.northeastern.edu.simpledb.backend.utils.Parser;

import java.util.Arrays;

/**
 * VM provide Entry to the upper layer
 * [XMIN][XMAX][data]
 */
public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;
    private static Entry newEntry(VersionManager vm, DataItem di, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = di;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = vm.dm.read(uid);
        return newEntry(vm, di, uid);
    }

    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() {
        vm.releaseEntry(this);
    }

    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray subArray = dataItem.data();
            byte[] data = new byte[subArray.end - (subArray.start + OF_DATA)];
            System.arraycopy(subArray.raw, subArray.start + OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray subArray = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(subArray.raw, subArray.start + OF_XMIN, subArray.start + OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray subArray = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(subArray.raw, subArray.start + OF_XMAX, subArray.start + OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray subArray = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, subArray.raw, subArray.start + OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    public long getUid() {
        return uid;
    }

    public void remove() {
        dataItem.release();
    }


}
