package com.northeastern.edu.simpledb.backend.vm;

import com.northeastern.edu.simpledb.backend.utils.Panic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import static org.junit.jupiter.api.Assertions.assertThrows;

class LockTableTest {
    @Spy
    Map<Long, List<Long>> x2u = new HashMap<>();
    @InjectMocks
    LockTable lockTable;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    /*
        eg:
        dfs(xid2)
        xid2 try to acquire uid1, but uid1 is holding by xid1
           dfs(xid1)
           xid1 is waiting for uid2, but uid2 is holding by xid2
     */
    @Test
    void testSingleAdd_expectedRuntimeException() throws Exception {
        try {
            lockTable.add(1, 1);
            lockTable.add(2, 2);
            lockTable.add(1, 2);
        } catch (Exception e) {
            Panic.panic(e);
        }

        assertThrows(RuntimeException.class, () -> lockTable.add(2, 1));
    }

    @Test
    void testRemove_expectedNoException() {
        try {
            lockTable.add(1, 1);
            lockTable.add(1, 2);
            lockTable.add(1, 3);
            lockTable.add(1, 4);
            lockTable.add(2, 4);
            lockTable.remove(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    void testMultiAdd_expectedRuntimeException() {
        for (long i = 1; i <= 100; i ++) {
            try {
                Lock o = lockTable.add(i, i);
                if(o != null) {
                    Runnable r = () -> {
                        o.lock();
                        o.unlock();
                    };
                    new Thread(r).start();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        for (long i = 1; i <= 99; i ++) {
            try {
                Lock o = lockTable.add(i, i+1);
                if(o != null) {
                    Runnable r = () -> {
                        o.lock();
                        o.unlock();
                    };
                    new Thread(r).start();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        assertThrows(RuntimeException.class, () -> lockTable.add(100, 1));
        lockTable.remove(23);

        try {
            lockTable.add(100, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }


    @Test
    void testPutIntoList_expectedNoException() {
        x2u.computeIfAbsent(1L, k -> new ArrayList<>()).add(0, 1L);
        assert x2u.size() > 0 && x2u.get(1L) != null && x2u.get(1L).size() == 1;
    }
}
