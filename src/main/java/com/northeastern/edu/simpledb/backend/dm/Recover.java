package com.northeastern.edu.simpledb.backend.dm;

import com.google.common.primitives.Bytes;
import com.northeastern.edu.simpledb.backend.common.SubArray;
import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;
import com.northeastern.edu.simpledb.backend.dm.dataItem.DataItem;
import com.northeastern.edu.simpledb.backend.dm.logger.Logger;
import com.northeastern.edu.simpledb.backend.dm.page.Page;
import com.northeastern.edu.simpledb.backend.dm.page.SecondaryPage;
import com.northeastern.edu.simpledb.backend.tm.TransactionManager;
import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.backend.utils.Parser;

import java.util.*;

public class Recover {

    /**
     * insert log: [size][checksum][log type][xid][page number][offset][raw]
     * update log: [size][checksum][log type][xid][uid][old data][latest data]
     */

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE =  1;

    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;

    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    private static final int OF_INSERT_PAGE_NUMBER = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PAGE_NUMBER + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    public static byte[] insertLog(long xid, Page page, byte[] raw) {
        // [DataType: 1byte][Xid: 8bytes][PageNumber: 4bytes][Offset: 2bytes][Raw]
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pageNumberRaw = Parser.int2Byte(page.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(SecondaryPage.getFSO(page));
        return Bytes.concat(logTypeRaw, xidRaw, pageNumberRaw, offsetRaw, raw);
    }

    static class InsertLogInfo {
        long xid;
        int pageNumber;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pageNumber;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger logger, PageCache pageCache) {
        System.out.println("Recovering");

        logger.rewind();
        int maxPage = 0;
        byte[] log;
        while ((log = logger.next()) != null) {
            int pageNumber;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pageNumber = li.pageNumber;
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                pageNumber = xi.pageNumber;
            }
            if (pageNumber > maxPage) {
                maxPage = pageNumber;
            }
        }

        if (maxPage == 0) maxPage = 1;
        pageCache.truncateByPageNumber(maxPage);
        System.out.println("Truncate to " + maxPage + " pages.");

        redoTransactions(tm, logger, pageCache);
        System.out.println("Redo Transactions Over.");

        undoTransactions(tm, logger, pageCache);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");

    }

    // redo transactions based on log file
    private static void redoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        logger.rewind();
        byte[] log;
        while ((log = logger.next()) != null) {
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                if (!tm.isActive(li.xid)) {
                    doInsertLog(pageCache, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                if (!tm.isActive(xi.xid)) {
                    doUpdateLog(pageCache, log, REDO);
                }
            }
        }
    }

    // [log type][xid][uid][old data][latest data]
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo updateLogInfo = new UpdateLogInfo();
        updateLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        // update id: [page number][offset]
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        updateLogInfo.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 16;
        updateLogInfo.pageNumber = (int) (uid & (1L << 32) - 1);
        int length = (log.length - OF_INSERT_RAW) / 2;
        updateLogInfo.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        updateLogInfo.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + 2 * length);
        return updateLogInfo;
    }

    // [log type][xid][page number][offset][raw]
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PAGE_NUMBER));
        insertLogInfo.pageNumber = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PAGE_NUMBER, OF_INSERT_OFFSET));
        insertLogInfo.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        insertLogInfo.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return insertLogInfo;
    }

    // determine type of log based on the first byte
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    // update transactions based on log file
    private static void undoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        HashMap<Long, List<byte[]>> logCache = new HashMap();
        logger.rewind();
        byte[] log;
        while ((log = logger.next()) != null) {
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                if (tm.isActive(li.xid)) {
                    if (!logCache.containsKey(li.xid)) logCache.put(li.xid, new ArrayList<>());
                    logCache.get(li.xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                if (tm.isActive(xi.xid)) {
                    if (!logCache.containsKey(xi.xid)) logCache.put(xi.xid, new ArrayList<>());
                    logCache.get(xi.xid).add(log);
                }
            }
        }

        // processing undo logs which state is active reversely
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] tmp = logs.get(i);
                if (isInsertLog(tmp)) doInsertLog(pageCache, tmp, UNDO);
                else doUpdateLog(pageCache, tmp, UNDO);
            }
            tm.abort(entry.getKey());
        }
    }


    private final static int REDO = 0;
    private final static int UNDO = 0;

    private static void doUpdateLog(PageCache pageCache, byte[] log, int flag) {
        // step1 determine action based on flag
        int pageNumber;
        short offset;
        byte[] raw;

        UpdateLogInfo updateLogInfo = parseUpdateLog(log);
        pageNumber = updateLogInfo.pageNumber;
        offset = updateLogInfo.offset;

        if (flag == REDO) raw = updateLogInfo.newRaw; // redo update
        else raw = updateLogInfo.oldRaw; // undo update

        // step2 get page by page number
        Page page = null;
        try {
            page = pageCache.getPage(pageNumber);
        } catch (Exception e) {
            Panic.panic(e);
        }

        // step3 update page with the help of `recoverUpdate()` from secondary page
        assert page != null;
        try {
            SecondaryPage.recoverUpdate(page, raw, offset);
        } finally {
            page.release();
        }
    }

    private static void doInsertLog(PageCache pageCache, byte[] log, int flag) {
        // step1 parsing insert log
        InsertLogInfo insertLogInfo = parseInsertLog(log);
        int pageNumber = insertLogInfo.pageNumber;

        // step2 determine action based on flag
        if (flag == REDO) DataItem.setDataItemRawInvalid(insertLogInfo.raw);

        // step3 get page by page number
        Page page = null;
        try {
            page = pageCache.getPage(pageNumber);
        } catch (Exception e) {
            Panic.panic(e);
        }

        // step4 update page with the help of `recoverInsert()` from secondary page
        assert page != null;
        try {
            SecondaryPage.recoverInsert(page, insertLogInfo.raw, insertLogInfo.offset);
        } finally {
            page.release();
        }
    }

    // when DataManger log into log file, data item should be wrapped in advance
    public static byte[] updateLog(long xid, DataItem dataItem) {
        byte[] logTypeRaw = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logTypeRaw, xidRaw, uidRaw, oldRaw, newRaw);
    }

}
