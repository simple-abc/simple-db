package com.northeastern.edu.simpledb.backend.tm;

import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public abstract class AbstractTransactionManager {
    abstract long begin();
    abstract void commit(long xid);
    abstract void abort(long xid);
    abstract boolean isActive(long xid);
    abstract boolean isCommitted(long xid);
    abstract boolean isAborted(long xid);
    abstract void close();

    public static TransactionManager create(String path) {
        File f = new File(path + TransactionManager.XID_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.BadXIDFileException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (IOException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.allocate(TransactionManager.XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManager(raf, fc);
    }

    public static TransactionManager open(String path) {
        File f = new File(path + TransactionManager.XID_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManager(raf, fc);
    }
}
