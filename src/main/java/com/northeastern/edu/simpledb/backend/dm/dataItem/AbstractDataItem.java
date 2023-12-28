package com.northeastern.edu.simpledb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import com.northeastern.edu.simpledb.backend.common.SubArray;
import com.northeastern.edu.simpledb.backend.dm.DataManger;
import com.northeastern.edu.simpledb.backend.dm.DataMangerHandler;
import com.northeastern.edu.simpledb.backend.dm.page.Page;
import com.northeastern.edu.simpledb.backend.utils.Parser;
import com.northeastern.edu.simpledb.backend.utils.Types;

import java.util.Arrays;

/**
 * AbstractDataItem
 * encapsulate data as DataItem and provide abstract objects
 * to the upper layer through DataManager
 */
public abstract class AbstractDataItem {

    abstract SubArray data();

    abstract void before();
    abstract void unBefore();
    abstract void after(long xid);
    abstract void release();

    abstract void lock();
    abstract void unlock();
    abstract void rLock();
    abstract void rUnLock();

    abstract Page page();
    abstract long getUid();
    abstract byte[] getOldRaw();
    abstract SubArray getRaw();

    // wrapping raw to DataItem
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // parsing raw to DataItem starting from the offset of page
    public static DataItem parseDataItem(Page page, short offset, DataManger dm) {
        byte[] raw = page.getData();
        short dataSize = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItem.OF_SIZE, offset + DataItem.OF_DATA));
        short length = (short) (dataSize + DataItem.OF_DATA);  // total length of data item
        long uid = Types.addressToUid(page.getPageNumber(), offset);
        return new DataItem(new SubArray(raw, offset, offset + length), new byte[length], page, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItem.OF_VALID] = 0b1;
    }

}
