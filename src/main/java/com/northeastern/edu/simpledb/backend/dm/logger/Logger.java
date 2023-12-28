package com.northeastern.edu.simpledb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.backend.utils.Parser;
import com.northeastern.edu.simpledb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.northeastern.edu.simpledb.common.Error.BadLogFileException;

public class Logger extends AbstractLogger{

    // a prime number for calculating check sum
    private static final int SEED = 13331;

    // starting position of attribute `size` in a log
    private static final int OF_SIZE = 0;

    // starting position of attribute `check sum` in a log
    private static final int OF_CHECKSUM = OF_SIZE + 4;

    // starting position of attribute `data` in a log
    private static final int OF_DATA = OF_CHECKSUM + 4;

    // standard suffix of log file
    public static final String LOG_SUFFIX = ".log";

    private Lock lock;
    private RandomAccessFile raf;
    private FileChannel fc;

    // the position of the pointer of logger
    private long position;

    // the size of log file, it will be updated when `open()` from disk
    private long fileSize;

    // the check sum of log file
    private int xCheckSum;

    public Logger(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        this.lock = new ReentrantLock();
    }

    public Logger(RandomAccessFile raf, FileChannel fc, int xCheckSum) {
        this.raf = raf;
        this.fc = fc;
        this.xCheckSum = xCheckSum;
        this.lock = new ReentrantLock();
    }

    /**
     * load check sum of log file, update fileSize as same as the
     * length of log file, and validate log file using check sum
     */
    void init() {
        long size = 0;
        try {
            size = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        if (size < 4) Panic.panic(BadLogFileException);

        // read size from log file
        ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        int xChecksum = Parser.parseInt(buf.array());
        this.fileSize = size;
        this.xCheckSum = xChecksum;

        checkAndRemoveTail();
    }

    // validate log file, remove invalid tail
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while (true) {
            // get the next log
            byte[] log = internNext();
            // calculate xChecksum accumulate
            if (log == null) break;
            xCheck = calChecksum(xCheck, log);
        }

        if (xCheck != xCheckSum) Panic.panic(Error.BadLogFileException);

        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            raf.seek(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        rewind();
    }

    // calculate check sum of log file, then flush to disk
    private void updateChecksum(byte[] log) {
        this.xCheckSum = calChecksum(this.xCheckSum, log);
        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(this.xCheckSum));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // wrap data as a log
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    /**
     * handle bad tail in two cases
     * case1: position + OF_DATA > raf.length || position + OF_DATA + data size > raf.length
     * case2: check sum1 != check sum2
     */
    private byte[] internNext() {
        if (position + OF_DATA >= fileSize) return null;

        // step1 check size of a log: [size][checksum][data]
        ByteBuffer tmp = ByteBuffer.allocate(4);

        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }

        int size = Parser.parseInt(tmp.array());
        if (position + OF_DATA + size > fileSize) return null;

        // step2 read whole check sum and data, then validate check sum
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);

        try {
            fc.position(position);
            fc.read(buf);
        } catch (Exception e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        int checksum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checksum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checksum1 != checksum2) return null;

        // step3 update position
        position += log.length;
        return log;
    }


    // calculate check sum based on xCheck and a log
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    @Override
    public void truncate(long position) throws IOException {
        lock.lock();
        try {
            fc.truncate(position);
        } finally {
            lock.unlock();
        }
    }

    // wrap data to log, write a log to log file, then update check sum of log file
    @Override
    public void log(byte[] data) {
        // step1 build log and its format is like `[size][checksum][data]`
        byte[] log = wrapLog(data);

        // step2 write log to file
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }

        // step3 update check sum
        updateChecksum(log);
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public long getPosition() {
        return this.position;
    }

    public FileChannel getFc() {
        return this.fc;
    };

}
