package com.northeastern.edu.simpledb.backend.tm;

import com.northeastern.edu.simpledb.backend.dm.page.Page;
import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerTest {

    static Random random = new SecureRandom();

    private int transCnt = 0;
    private static final int noWorks = 300;
    private static final int noWorkers = 50;
    private static TransactionManager tm;
    private static Map<Long, Byte> transMap;
    private static Lock lock;
    private static CountDownLatch cdl;


    @BeforeAll
    static void setup() {
        tm = TransactionManager.create("tm-test");
        lock = new ReentrantLock();
        cdl = new CountDownLatch(noWorkers);
        transMap = new ConcurrentHashMap<>();
    }

    @AfterAll
    static void cleanTestEnv() {
        assert new File("tm-test" + TransactionManager.XID_SUFFIX).delete();
    }

    /**
     * let's suppose ConcurrentHashMap is thread-safe, randomly
     * starting a transaction, and put xid, status into ConcurrentHashMap
     * at same time, compare the state in ConcurrentHashMap with
     * state provided by Transaction Manager randomly.
     */
    @Test
    void testMultiThread_expectedNoException() {
        for (int i = 0; i < noWorkers; i++) {
            Runnable r = () -> worker();
            new Thread(r).run();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void worker() {
        boolean inTans = false;
        long transXID = 0;
        for (int i = 0; i < noWorks; i++) {
            int op = Math.abs(random.nextInt(6));
            if (op == 0) { // do transaction
                lock.lock();
                if (!inTans) { // starting a transaction synchronously in TM and ConcurrentHashMap
                    long xid = tm.begin();
                    transMap.put(xid, (byte) 0);
                    transCnt++;
                    transXID = xid;
                    inTans = true;
                } else { // determine if commit or abort transaction randomly
                    int state = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch (state) {
                        case 1:
                            tm.commit(transXID);
                            break;
                        case 2:
                            tm.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte) state);
                    inTans = false;
                }
                lock.unlock();
            } else { // check state
                lock.lock();
                if (transCnt > 0) {
                    long xid = (long) ((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
                    byte state = transMap.get(xid);
                    boolean ok = false;
                    switch (state) {
                        case 0:
                            ok = tm.isActive(xid);
                            break;
                        case 1:
                            ok = tm.isCommitted(xid);
                            break;
                        case 2:
                            ok = tm.isAborted(xid);
                            break;
                    }
                    assert ok;
                }
                lock.unlock();
            }
        }
        cdl.countDown();
    }
}
