package com.northeastern.edu.simpledb.backend.pageIndex;

import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;
import com.northeastern.edu.simpledb.backend.dm.pageIndex.PageIndex;
import com.northeastern.edu.simpledb.backend.dm.pageIndex.PageInfo;
import org.junit.jupiter.api.Test;

public class PageIndexTest {

    @Test
    void testPageIndex_expectedNoException() {
        PageIndex pageIndex = new PageIndex();
        int threshold = PageCache.PAGE_SIZE / 40;
        for (int i = 0; i < 40; i++) {
            pageIndex.add(i, i * threshold);
            pageIndex.add(i, i * threshold);
            pageIndex.add(i, i * threshold);
        }

        for (int i = 0; i < 39; i++) {
            PageInfo pageInfo = pageIndex.select(i * threshold);
            assert pageInfo != null;
            assert pageInfo.pageNumber == i + 1;
        }
    }
}
