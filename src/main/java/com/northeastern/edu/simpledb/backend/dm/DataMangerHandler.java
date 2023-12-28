package com.northeastern.edu.simpledb.backend.dm;

import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;
import com.northeastern.edu.simpledb.backend.dm.cache.PageCacheHandler;
import com.northeastern.edu.simpledb.backend.dm.dataItem.DataItem;
import com.northeastern.edu.simpledb.backend.dm.logger.Logger;
import com.northeastern.edu.simpledb.backend.dm.page.L1Page;
import com.northeastern.edu.simpledb.backend.tm.TransactionManager;

public interface DataMangerHandler {

    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /**
     * create page cache, logger, and data manager
     * initialize the L1 page
     */
    static DataManger create(String path, long memory, TransactionManager tm) {
        PageCache pageCache = PageCacheHandler.create(path, memory);
        Logger logger = Logger.create(path);

        DataManger dataManger = new DataManger(pageCache, logger, tm);
        dataManger.initL1Page();
        return dataManger;
    }

    static DataManger open(String path, long memory, TransactionManager tm) {
        PageCache pageCache = PageCacheHandler.open(path, memory);
        Logger logger = Logger.open(path);

        DataManger dataManger = new DataManger(pageCache, logger, tm);
        if (!dataManger.loadCheckFirstPage()) {   // check failed
            Recover.recover(tm, logger, pageCache); // recover based on log file
        }
        dataManger.fillPageIndex(); // initialize page index
        L1Page.setVcOpen(dataManger.firstPage); // set flag for valid check
        dataManger.pageCache.flushPage(dataManger.firstPage); // flush to disk immediately
        return dataManger;
    }
}
