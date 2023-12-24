package com.northeastern.edu.simpledb.backend.dm.cache;

import com.northeastern.edu.simpledb.backend.dm.page.Page;
import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCacheHandler {

    int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);
    Page getPage(int pageNumber) throws Exception;
    void close();
    void release(Page page);

    void truncateByPageNumber(int maxPageNumber);
    int getPageNumber();
    void flushPage(Page pg);

    static PageCache create(String path, long memory) {
        File f = new File(path + PageCache.DB_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCache(raf, fc, (int) memory / PAGE_SIZE);
    }

    static PageCache open(String path, long memory) {
        File f = new File(path + PageCache.DB_SUFFIX);
        if (!f.exists()) Panic.panic(Error.FileNotExistsException);
        if (!f.canRead() || !f.canWrite()) Panic.panic(Error.FileCannotRWException);

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCache(raf, fc, (int) memory / PAGE_SIZE);
    }
}
