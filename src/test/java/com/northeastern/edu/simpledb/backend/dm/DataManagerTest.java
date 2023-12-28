package com.northeastern.edu.simpledb.backend.dm;

import com.northeastern.edu.simpledb.backend.common.SubArray;
import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;
import com.northeastern.edu.simpledb.backend.dm.dataItem.DataItem;
import com.northeastern.edu.simpledb.backend.tm.TransactionManager;
import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.backend.utils.RandomUtil;
import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.northeastern.edu.simpledb.backend.dm.cache.PageCache.DB_SUFFIX;
import static com.northeastern.edu.simpledb.backend.dm.logger.Logger.LOG_SUFFIX;

public class DataManagerTest {

    private static final String XID_SUFFIX = ".xid";
    private static final int workerNum = 10;
    private static final int tasksNum = 50;

    private static final int totalData = 10;
    static CountDownLatch cdl;
    static TransactionManager tm;

    static DataManger dm;

    static Lock lock;

    static List<Long> uids;

    static Random random = new SecureRandom();
    static ConcurrentHashMap<Integer, String> map;

    @BeforeAll
    static void setup() {
        uids = new ArrayList<>();
        tm = TransactionManager.create("dm-test");
        dm = DataMangerHandler.create("dm-test", PageCache.PAGE_SIZE * 10, tm);
        cdl = new CountDownLatch(workerNum);
        lock = new ReentrantLock();
        map = new ConcurrentHashMap<>();

        for (int i = 0; i < tasksNum; i++) {
            String s = UUID.randomUUID().toString();
            map.put(i, s);
        }
    }

    @AfterAll
    static void cleanTestEnv() {
        new File("dm-test" + LOG_SUFFIX).delete();
        new File("dm-test" + DB_SUFFIX).delete();
        new File("dm-test" + XID_SUFFIX).delete();
    }

    @Test
    void testMultiThread_expectedMapContainsAllElements() {
        for (int i = 0; i < workerNum; i++) {
            new Thread(() -> worker()).start();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private static final int dataRotation = 50;

    private void worker() {
        try {
            for (int i = 0; i < tasksNum; i++) {
                int v;
                if ((v = random.nextInt() % 100) > dataRotation) {
                    String s = map.get(random.nextInt(tasksNum));
                    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                    long uid = dm.insert(0, bytes);
                    try {
                        lock.lock();
                        uids.add(uid);
                    } finally {
                        lock.unlock();
                    }
                } else {
                    try {
                        lock.lock();
                        if (uids.size() == 0) continue;
                    } finally {
                        lock.unlock();
                    }
                    int tmp = Math.abs(random.nextInt()) % uids.size();
                    Long uid = uids.get(tmp);
                    DataItem dataItem = dm.read(uid);

                    dataItem.rLock();
                    SubArray sa = dataItem.data();
                    byte[] bytes = Arrays.copyOfRange(sa.raw, sa.start, sa.end);
                    Assertions.assertTrue(map.contains(new String(bytes)));
                    dataItem.rUnLock();
                }
            }
        } catch (Exception e){
            Panic.panic(e);
        } finally {
            cdl.countDown();
        }
    }
}
