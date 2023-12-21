package com.northeastern.edu.simpledb.backend.common;

import com.northeastern.edu.simpledb.common.Error;

import java.util.Objects;
import java.util.concurrent.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

public class AbstractCacheTest {

    private static final String SERIALIZE_SUFFIX = ".ser";

    private static final Integer THREAD_COUNT = 8;

    @BeforeAll
    static void prepareDataForTest() {

        for (int i = 1; i <= 2; i++) {
            // create an instance of the Data class
            Data data = new Data();
            data.setContent(String.valueOf(i));

            // specify the file path where you want to save the serialized object
            String filePath = data.getContent() + SERIALIZE_SUFFIX;

            // serialize the object and write it to the file
            try (FileOutputStream fileOut = new FileOutputStream(filePath);
                 ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {

                // write the object to the output stream
                objectOut.writeObject(data);

                System.out.println("Object has been serialized and written to " + filePath);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    void testCacheIsFull_throwCacheFullException() throws Exception {

        int maxResource = 1;
        MyAbstractCache cache = new MyAbstractCache(maxResource);

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch countDownLatch = new CountDownLatch(THREAD_COUNT);

        Future<?>[] futures = new Future[THREAD_COUNT];

        assertThrows(Error.CacheFullException.getClass(), () -> {
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int finalI = i;
                futures[i] = executorService.submit(() -> {
                    countDownLatch.countDown();
                    try {
                        countDownLatch.await();
                        Data data = cache.get(finalI % 2 == 0 ? 1L : 2L);
                        if (data != null) System.out.println("data content = " + data.getContent());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            for (int i = 0; i < THREAD_COUNT; i++) {
                try {
                    futures[i].get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        executorService.shutdown();
        executorService.awaitTermination(2, TimeUnit.SECONDS);
    }

    @Test
    void testMultiThreadGet_expectedNumberOfOutputEqualToThreadCount() throws Exception {
        int maxResource = 2;
        MyAbstractCache cache = new MyAbstractCache(maxResource);

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch countDownLatch = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int finalI = i;
            executorService.submit(() -> {
                countDownLatch.countDown();
                try {
                    countDownLatch.await();
                    Data data = cache.get(finalI % 2 == 0 ? 1L : 2L);
                    if(data != null) System.out.println("data content = " + data.getContent());
                } catch (Exception e) {
                    assertEquals(Error.CacheFullException, e);
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(2, TimeUnit.SECONDS);
    }

    @Test
    void testCacheRelease_expectedOnlyOneSerialization() throws InterruptedException {
        int maxResource = 2;
        MyAbstractCache cache = new MyAbstractCache(maxResource);

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch countDownLatch = new CountDownLatch(THREAD_COUNT);

        // submit multiple threads trying to access the same key concurrently
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int finalI = i;
            executorService.submit(() -> {
                countDownLatch.countDown();
                try {
                    countDownLatch.await();
                    Data data = cache.get(1L);
                    if (Thread.currentThread().getId() % 2 == 0) TimeUnit.SECONDS.sleep(2);
                    System.out.println("Thread " + Thread.currentThread().getId() + " got data: " + data.getContent());
                    // update data
                    data.setContent(String.valueOf(finalI));
                    // release the cache after processing
                    cache.release(1L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(2, TimeUnit.SECONDS);
    }
}

class MyAbstractCache extends AbstractCache<Data> {

    private static final String SERIALIZE_SUFFIX = ".ser";

    public MyAbstractCache(int maxResource) {
        super(maxResource);
    }

    @Override
    protected Data getForCache(long key) {
        Data data = null;

        // specify the file path where the serialized object is stored
        String filePath = key + SERIALIZE_SUFFIX;

        // deserialize the object from the file
        try (FileInputStream fileIn = new FileInputStream(filePath);
             ObjectInputStream objectIn = new ObjectInputStream(fileIn)) {

            // read the object from the input stream
            data = (Data) objectIn.readObject();

            // access the content of the deserialized object
            System.out.println("Deserialized content: " + data.getContent());

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return data;
    }

    @Override
    protected void releaseForCache(Data obj) {

        // specify the file path where you want to save the serialized object
        String filePath = obj.getContent() + SERIALIZE_SUFFIX;

        // serialize the object and write it to the file
        try (FileOutputStream fileOut = new FileOutputStream(filePath);
             ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {

            // write the object to the output stream
            objectOut.writeObject(obj);

            System.out.println("Object has been serialized and written to " + filePath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

class Data implements Serializable {

    public static final long SERIALIZE_ID = 123456789L;

    private String content;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Data data = (Data) o;
        return Objects.equals(content, data.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(content);
    }
}