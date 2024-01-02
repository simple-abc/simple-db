package com.northeastern.edu.simpledb.backend.im;

import com.northeastern.edu.simpledb.backend.dm.DataManger;
import com.northeastern.edu.simpledb.backend.dm.DataMangerHandler;
import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;
import com.northeastern.edu.simpledb.backend.tm.TransactionManager;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

public class BPlusTreeTest {

    @Test
    void testBuildTree_expectedNoException() throws Exception {
        DataManger dm = DataMangerHandler.create("test-bptree", PageCache.PAGE_SIZE * 10, TransactionManager.create("test-bptree"));

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

        assert new File("test-bptree.db").delete();
        assert new File("test-bptree.log").delete();
        assert new File("test-bptree.xid").delete();

    }
}
