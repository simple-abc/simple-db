package com.northeastern.edu.simpledb.backend.dm.page;

import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;
import com.northeastern.edu.simpledb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * L1 Page
 * when db start up, write random bytes from OF_VC to (OF_VC + LEN_VC - 1) byte
 * when db close, copy random bytes from (OF_VC + LEN_VC) to (OF_VC + 2 * LEN_VC - 1) byte
 * used to determine whether the database was shut down normally
 * last time
 */
public class L1Page {

    private final static int OF_VC = 100;

    private final static int LEN_VC = 8;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page page) {
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page page) {
        page.setDirty(true);
        setVcClose(page.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC), Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC));
    }

}
