package com.northeastern.edu.simpledb.backend.dm.pageIndex;

import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Page Index
 * the page index caches the free space of each page. It is
 * used to quickly find a page with suitable space when the
 * upper module performs an insertion operation without checking
 * the information of each page from the disk or cache
 */
public class PageIndex {

    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;

    /**
     * entry in Map
     * key: the number of free slot : Integer
     * value: all of PageInfo have the number of free slot : List
     */
    private List<PageInfo>[] lists; 

    @SuppressWarnings("unchecked")
    public PageIndex() {
        this.lock = new ReentrantLock();
        this.lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }


    public void add(int pageNumber, int freeSpace) {
        int number = freeSpace / THRESHOLD; // how many free slot this page has?
        lists[number].add(new PageInfo(pageNumber, freeSpace));
    }

    /**
     * calculating how many free slot needed, then
     * accessing the next one, having more slot and
     * ensuring the data won't across two pages.
     */
    public PageInfo select(int spaceSize) {
        int number = spaceSize / THRESHOLD; // how many free slot need?
        if (number < INTERVALS_NO) number++; // rounded up

        // iterate over lists finding the first page doesn't acquire by other thread
        while (number <= INTERVALS_NO) {
            if (lists[number].size() == 0) {
                number++;
                continue;
            }
            if (lists[number].size() > 0) return lists[number].remove(0);
        }
        return null;
    }


}
