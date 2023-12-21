package com.northeastern.edu.simpledb.backend.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.northeastern.edu.simpledb.common.Error;

/**
 * AbstractCache is a reference counting strategy for caching
 */
public abstract class AbstractCache<T> {

    // the actual cache for data
    private Map<Long, T> cache;

    // a counter for the reference
    private Map<Long, Integer> references;

    // whether the resource is being obtained by other thread
    private Map<Long, Boolean> getting;

    // the maximum number of data in cache
    private int maxResource;

    // the number of elements in the cache
    private int count = 0;

    private Lock lock;

    private Condition condition;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        this.cache = new ConcurrentHashMap<>();
        this.references = new ConcurrentHashMap<>();
        this.getting = new ConcurrentHashMap<>();
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }

    // obtain data based on key
    protected T get(long key) throws Exception {

        lock.lock();

        try {
            // the thread starts waiting while the data the thread wants is being loaded into the cache
            while (getting.containsKey(key)) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            if (cache.containsKey(key)) {
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                return obj;
            }

            if(maxResource > 0 && count == maxResource) {
                throw Error.CacheFullException;
            }

            count++;
            getting.put(key, true);
        } catch (Exception e) {
            throw e;
        } finally {
            lock.unlock();
        }


        T obj = null;
        try {
            obj = getForCache(key);
        } catch(Exception e) {
            lock.lock();
            try {
                count--;
                getting.remove(key);
            } finally {
                lock.unlock();
            }
            throw e;
        }

        lock.lock();
        try {
            getting.remove(key);
            cache.put(key, obj);
            references.put(key, 1);
            // after data is loaded into the cache, notify all threads is sleeping before
            condition.signalAll();
        } finally {
            lock.unlock();
        }

        return obj;
    }

    // forcefully release a cache based on key
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref < 0) return ;
            if(ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    // turn off caching and write back all resources
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    // default behavior to perform if cache misses
    protected abstract T getForCache(long key);

    // default write back behavior when cache eviction
    protected abstract void releaseForCache(T obj);
}
