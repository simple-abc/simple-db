package com.northeastern.edu.simpledb.backend.vm;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.northeastern.edu.simpledb.common.Error;
import com.northeastern.edu.simpledb.common.Error.*;

public class LockTable {

    /**
     * what uids that xid is holding
     * format: {xid: [uid0, uid1, uid2...]}
     */
    private Map<Long, List<Long>> x2u;

    /**
     * what xid is holding the uid
     * format: {uid: xid}
     */
    private Map<Long, Long> u2x;

    /**
     * all of xid are waiting uid
     * format: {uid: [xid0, xid1, xid2...]}
     */
    private Map<Long, List<Long>> wait;

    /**
     * the lock of xid is waiting resource
     * format: {xid: Lock}
     */
    private Map<Long, Lock> waitLock;

    /**
     * what uid that xid is waiting for
     * format:  {xid : uid}
     */
    private Map<Long, Long> waitU;

    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // add a relationship which is xid is waiting uid
    public Lock add(long xid, long uid) throws Exception{
        lock.lock();
        try {
            // xid is holding uid
            if (isInList(x2u, xid, uid)) return null;

            // if no one is holding uid, add the relationship directly (acquired resource)
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }

            // other is holding uid
            // try to wait
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);

            // check deadlock
            if (hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadLockException;
            }

            // no deadlock, start waiting
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l); // store lock for the releasing later
            return l;
        } finally {
            lock.unlock();
        }
    }

    // remove a relationship between xid and uid
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> uids = x2u.get(xid);
            if (uids != null) {
                while (uids.size() > 0) {
                    Long uid = uids.remove(0);
                    selectNewXID(uid); // select the next xid for uid from candidates
                }
            }
        } finally {
            lock.unlock();
        }
    }

    // select the next xid for uid from candidates
    private void selectNewXID(Long uid) {
        u2x.remove(uid); // remove the relationship which is uid is acquired by xid
        List<Long> xids = wait.get(uid); // get candidates queue
        if (xids == null) return ;
        assert xids.size() > 0;

        while (xids.size() > 0) {
            Long xid = xids.remove(0);
            if (!waitLock.containsKey(xid)) { // no lock found for the xid
                continue;
            } else { // assign uid this xid
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                Lock l = waitLock.remove(xid);
                waitU.remove(xid);
                l.unlock();
            }
        }
    }

    private void removeFromList(Map<Long, List<Long>> wait, long uid, long xid) {
        List<Long> list = wait.getOrDefault(uid, List.of());
        Iterator<Long> iterator = list.iterator();
        while (iterator.hasNext()) {
            Long e;
            if ((e = iterator.next()) == xid) {
                iterator.remove();
                break;
            }
        }
        if (list.size() == 0) wait.remove(uid);
    }

    private void putIntoList(Map<Long, List<Long>> map, long key, long value) {
        map.computeIfAbsent(key, k -> new ArrayList<>()).add(0, value);
    }

    private boolean isInList(Map<Long, List<Long>> x2u, long xid, long uid) {
        return x2u.getOrDefault(xid, List.of()).stream().anyMatch(e -> e == uid);
    }


    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        // iterate x2u
        for (long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if (s != null && s > 0) continue;
            stamp++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    /**
     *  see what uid that the xid is waiting for, and
     *  check if another xid holding the uid is waiting
     *  the uid acquired by the xid
     */
    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid); // what uid that this xid is waiting for
        if(uid == null) return false;
        Long x = u2x.get(uid); // what xid is holding this uid
        assert x != null;
        return dfs(x);
    }
}
