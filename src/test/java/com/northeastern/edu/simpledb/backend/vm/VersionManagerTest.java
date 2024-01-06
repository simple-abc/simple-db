package com.northeastern.edu.simpledb.backend.vm;

import com.northeastern.edu.simpledb.backend.dm.DataManger;
import com.northeastern.edu.simpledb.backend.dm.DataMangerHandler;
import com.northeastern.edu.simpledb.backend.dm.cache.PageCache;
import com.northeastern.edu.simpledb.backend.tm.TransactionManager;
import org.junit.jupiter.api.*;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static com.northeastern.edu.simpledb.backend.dm.cache.PageCache.DB_SUFFIX;
import static com.northeastern.edu.simpledb.backend.dm.logger.Logger.LOG_SUFFIX;
import static com.northeastern.edu.simpledb.backend.tm.TransactionManager.XID_SUFFIX;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.when;

class VersionManagerTest {

    private static final String TEST_NAME = "vm-test";

    static TransactionManager tm;
    static DataManger dm;
    static VersionManagerHandler vm;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @BeforeAll
    static void setUpEnv() {
        tm = TransactionManager.create(TEST_NAME);
        dm = DataMangerHandler.create(TEST_NAME, PageCache.PAGE_SIZE * 10, tm);
        vm = VersionManagerHandler.newVersionManager(tm, dm);
    }

    @AfterAll
    static void cleanTestEnv() {
        new File(TEST_NAME + LOG_SUFFIX).delete();
        new File(TEST_NAME + DB_SUFFIX).delete();
        new File(TEST_NAME + XID_SUFFIX).delete();
    }

    @Test
    void testRead_expectedNoException() throws Exception {
        long xid = vm.begin(1);
        long uid = vm.insert(xid, "version manager read test".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(new String(vm.read(xid, uid)), "version manager read test");
        vm.commit(xid);
    }

    @Test
    void testDelete_expectedNoException() throws Exception {
        long xid = vm.begin(1);
        long uid = vm.insert(xid, "version manager delete test".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(vm.delete(xid, uid), Boolean.TRUE);
        Assertions.assertNull(vm.read(xid, uid));
        vm.commit(xid);
    }

    @Test
    void testAbort_expectedRollBackSuccess() throws Exception {
        long xid = vm.begin(1);
        long uid = vm.insert(xid, "version manager abort test".getBytes(StandardCharsets.UTF_8));
        vm.abort(xid);
        Assertions.assertNull(vm.read(xid, uid));
    }

}