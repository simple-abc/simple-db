package com.northeastern.edu.simpledb.backend.im;

import com.northeastern.edu.simpledb.backend.dm.DataManger;
import com.northeastern.edu.simpledb.backend.dm.DataMangerHandler;
import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;
import com.northeastern.edu.simpledb.backend.tm.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static com.northeastern.edu.simpledb.backend.dm.cache.PageCache.DB_SUFFIX;
import static com.northeastern.edu.simpledb.backend.dm.logger.Logger.LOG_SUFFIX;
import static com.northeastern.edu.simpledb.backend.tm.TransactionManager.XID_SUFFIX;

public class BPlusTreeTest {

    private static final String TEST_NAME = "bptree-test";

    @AfterAll
    static void cleanTestEnv() {
        new File(TEST_NAME + LOG_SUFFIX).delete();
        new File(TEST_NAME + DB_SUFFIX).delete();
        new File(TEST_NAME + XID_SUFFIX).delete();
    }

    @Test
    void testBuildTree_expectedNoException() throws Exception {
        DataManger dm = DataMangerHandler.create(TEST_NAME, PageCache.PAGE_SIZE * 10, TransactionManager.create(TEST_NAME));

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 15;
        for (int i = 1; i <= lim; i++) {
            tree.insert(i, i);
        }
        for (int i = 1; i <= lim; i++) {
            List<Long> uids = tree.search(i);
            // System.out.println("=============================");
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }

        /*
            tree.insert(16, 16);
            tree.insert(17, 17);
            tree.insert(18, 18);
            List<Long> uids = tree.search(6);
        */
    }
}
