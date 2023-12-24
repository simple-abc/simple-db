package com.northeastern.edu.simpledb.backend.dm.page;

import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;
import com.northeastern.edu.simpledb.backend.utils.Parser;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class SecondaryPage {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    // insert raw into page, return insertion position
    public static short insert(Page page, byte[] raw) {
        page.setDirty(true);
        // get free space offset
        short offset = getFSO(page.getData());
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        // update free space offset
        setFSO(raw, (short)(offset + raw.length));
        return offset;
    }

    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, OF_FREE, OF_DATA));
    }

    public static short getFSO(Page page) {
        return getFSO(page.getData());
    }

    public static int getFreeSpace(Page page) {
        return PageCache.PAGE_SIZE - (int) getFSO(page.getData());
    }

    /**
     * recover insert statement from log file
     * update FSO of page if needed
     */
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);

        short rawFSO = getFSO(page.getData());
        if (rawFSO < offset + raw.length) setFSO(page.getData(), (short) (offset + raw.length));
    }

    // recover update statement from log file
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }

}

