package com.northeastern.edu.simpledb.backend.dm;

import com.northeastern.edu.simpledb.backend.common.AbstractCache;
import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;
import com.northeastern.edu.simpledb.backend.dm.dataItem.DataItem;
import com.northeastern.edu.simpledb.backend.dm.logger.Logger;
import com.northeastern.edu.simpledb.backend.dm.page.L1Page;
import com.northeastern.edu.simpledb.backend.dm.page.Page;
import com.northeastern.edu.simpledb.backend.dm.page.SecondaryPage;
import com.northeastern.edu.simpledb.backend.dm.pageIndex.PageIndex;
import com.northeastern.edu.simpledb.backend.dm.pageIndex.PageInfo;
import com.northeastern.edu.simpledb.backend.tm.TransactionManager;
import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.backend.utils.Types;
import com.northeastern.edu.simpledb.common.Error;

public class DataManger extends AbstractCache<DataItem> implements DataMangerHandler {

    TransactionManager tm;
    PageCache pageCache;
    Logger logger;
    PageIndex pageIndex;

    Page firstPage;

    public DataManger(PageCache pageCache, Logger logger, TransactionManager tm) {
        super(0);
        this.pageCache = pageCache;
        this.logger = logger;
        this.tm = tm;
        this.pageIndex = new PageIndex();
    }

    void initL1Page() {
        int pageNumber = pageCache.newPage(L1Page.initRaw());
        assert pageNumber == 1;
        try {
            firstPage = pageCache.getPage(pageNumber);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pageCache.flushPage(firstPage);
    }

    boolean loadCheckFirstPage() {
        try {
            firstPage = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return L1Page.checkVc(firstPage);
    }

    // initialize page index after DataManger#open
    void fillPageIndex() {
        int pageNumber = pageCache.getPageNumber();
        // iterate over all page number, then build page index
        for (int i = 2; i <= pageNumber; i++) {
            Page page = null;
            try {
                page = pageCache.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(pageNumber, SecondaryPage.getFreeSpace(page));
            page.release();
        }
    }

    // generate log for every transaction referring to `xid`
    public void logDataItem(long xid, DataItem dataItem) {
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }

    // build key from uid, then get data item by key
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pageNumber = (int) (uid & ((1L << 32) - 1));
        Page page = pageCache.getPage(pageNumber);
        return DataItem.parseDataItem(page, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    // read a data by uid
    @Override
    public DataItem read(long uid) throws Exception {
        DataItem dataItem = super.get(uid);
        if (!dataItem.isValid()) {
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    // insert a data through DataManger
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > SecondaryPage.MAX_FREE_SPACE) throw Error.DataTooLargeException;

        // TODO: need to refactor
        // step1 find page index
        PageInfo pageInfo = null;
        for (int i = 0; i < 5; i++) {
            pageInfo = pageIndex.select(raw.length);
            if (pageInfo != null) {
                break;
            } else {
                // if no page has enough free space, create a new secondary page
                int newPageNumber = pageCache.newPage(SecondaryPage.initRaw());
                pageIndex.add(newPageNumber, SecondaryPage.MAX_FREE_SPACE);
            }
        }

        if (pageInfo == null) throw Error.DataBaseBusyException;

        // step2 write to log file
        Page page = null;
        int freeSpace = 0;

        try {
            page = pageCache.getPage(pageInfo.pageNumber);
            byte[] log = Recover.insertLog(xid, page, raw);
            logger.log(log);

            // step3 write to page, return offset can be used as uid for cache of data item
            short offset = SecondaryPage.insert(page, raw);
            page.release();
            return Types.addressToUid(pageInfo.pageNumber, offset);
        } finally {
            // step4 put back page index
            if (page != null) {
                pageIndex.add(pageInfo.pageNumber, SecondaryPage.getFreeSpace(page));
            } else {
                pageIndex.add(pageInfo.pageNumber, freeSpace);
            }
        }

    }

    // close cache and logger
    @Override
    public void close() {
        super.close();
        logger.close();

        L1Page.setVcClose(firstPage);
        firstPage.release();
        pageCache.close();
    }

    public void releaseDataItem(DataItem dataItem) {
        super.release(dataItem.getUid());
    }
}
