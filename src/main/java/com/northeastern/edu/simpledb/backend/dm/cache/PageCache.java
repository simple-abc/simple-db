package com.northeastern.edu.simpledb.backend.dm.cache;

import com.northeastern.edu.simpledb.backend.common.AbstractCache;
import com.northeastern.edu.simpledb.backend.dm.page.Page;
import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCache extends AbstractCache<Page> implements PageCacheHandler {

    private static final int MEM_MIN_LIM = 10;

    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;

    private FileChannel fc;

    private Lock fileLock;

    private AtomicInteger pageNumbers;

    public PageCache(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM) Panic.panic(Error.MemTooSmallException);
        long length = 0;
        try {
            length = file.length();
        } catch (Exception e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    // turn off caching and write back all resources
    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * read page from database file based on pageNumber, and
     * encapsulate it as Page
     */
    @Override
    protected Page getForCache(long key) {
        int pageNumber = (int) key;
        long offset = pageOffset(pageNumber);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
        return new Page(pageNumber, buf.array(), this);
    }

    // calculate offset from the beginning based on page number
    static long pageOffset(int pageNumber) {
        return (long) (pageNumber - 1) * PAGE_SIZE;
    }

    // release cache, and write back to disk
    @Override
    protected void releaseForCache(Page page) {
        if (page.isDirty()) {
            flushPage(page);
            page.setDirty(false);
        }
    }

    // create a new page based on data, then flush to disk
    @Override
    public int newPage(byte[] initData) {
        int pageNumber = pageNumbers.incrementAndGet();
        Page page = new Page(pageNumber, initData, null);
        flush(page);
        return pageNumber;
    }

    // get page from cache by page number
    @Override
    public Page getPage(int pageNumber) throws Exception {
        return get(pageNumber);
    }

    // release page from cache
    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    // truncate file based on max page number
    @Override
    public void truncateByPageNumber(int maxPageNumber) {
        long size = pageOffset(maxPageNumber + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNumber);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }

    private void flush(Page page) {
        int pageNumber = page.getPageNumber();
        long offset = pageOffset(pageNumber);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }
}
