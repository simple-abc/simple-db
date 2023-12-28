package com.northeastern.edu.simpledb.backend.dm;

import com.northeastern.edu.simpledb.backend.common.SubArray;
import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;
import com.northeastern.edu.simpledb.backend.dm.dataItem.DataItem;
import com.northeastern.edu.simpledb.backend.dm.logger.Logger;
import com.northeastern.edu.simpledb.backend.tm.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.northeastern.edu.simpledb.backend.dm.cache.PageCache.DB_SUFFIX;
import static com.northeastern.edu.simpledb.backend.dm.logger.Logger.LOG_SUFFIX;

public class RecoverTest {

    private static final String XID_SUFFIX = ".xid";
    private static DataManger dm;
    private static TransactionManager tm;

    private static long xid;

    @BeforeAll
    static void setup() {
        tm = TransactionManager.create("tm-test");
        dm = DataMangerHandler.create("dm-test", PageCache.PAGE_SIZE * 10, tm);
        xid = tm.begin();
    }

    @AfterAll
    static void cleanTestEnv() {
        new File("dm-test" + LOG_SUFFIX).delete();
        new File("dm-test" + DB_SUFFIX).delete();
        new File("tm-test" + XID_SUFFIX).delete();
    }

    @Test
    void testRecover_expectedRedoWorks() {
        long uid;
        try {
            uid = dm.insert(xid, "hello db!".getBytes(StandardCharsets.UTF_8));
            DataItem dataItem0 = dm.read(uid);
            SubArray data0 = dataItem0.data();
            byte[] bytes = Arrays.copyOfRange(data0.raw, data0.start, data0.end);
            String actualStr = new String(bytes);
            Assertions.assertEquals("hello db!", actualStr);

            Field loggerFiled = dm.getClass().getDeclaredField("logger");
            loggerFiled.setAccessible(true);
            Logger reloadedLog = Logger.open("dm-test");
            loggerFiled.set(dm, reloadedLog);

            Field pageCacheField = dm.getClass().getDeclaredField("pageCache");
            pageCacheField.setAccessible(true);
            PageCache pageCache = (PageCache) pageCacheField.get(dm);

            Recover.recover(tm, reloadedLog, pageCache);

            Assertions.assertNull(dm.read(uid));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
