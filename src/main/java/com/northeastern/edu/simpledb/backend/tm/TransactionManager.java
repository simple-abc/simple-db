package com.northeastern.edu.simpledb.backend.tm;

import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.backend.utils.Parser;
import com.northeastern.edu.simpledb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/*

*/
public class TransactionManager extends AbstractTransactionManager{

    // the length of header of xid file, unit is byte
    static final int XID_HEADER_LENGTH = 8;

    // the size of each xid, unit is byte
    private static final int XID_FIELD_SIZE = 1;

    // three states of transaction
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    // super transaction whose state is always committed
    private static final long SUPER_XID = 0;

    // standard suffix of xid file
    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    public TransactionManager(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        this.counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * check if the xid file is valid
     * read the xidcounter in XID_FILE_HEADER, calculate
     * the theoretical length of the file based on it, and
     * compare it with the actual length
     */
    private void checkXIDCounter() {
        long fileLength = 0;
        try {
            fileLength = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }
        if (fileLength < XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = buf.getLong();
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLength) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /**
     * calculate its position in xid file based on input `xid`
     * `xid` means it is the normal xid transaction starting from 1
     * the xid of super transaction is 0
     */
    private long getXidPosition(long xid) {
        if (xid == 0) {
            throw new RuntimeException("position of xid can't be zero");
        }
        return (xid - 1) * XID_FIELD_SIZE + XID_HEADER_LENGTH;
    }

    // update state by xid
    private void updateXID(long xid, byte state) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = state;
        try {
            fc.position(offset);
            fc.write(ByteBuffer.wrap(tmp));
        } catch (IOException e) {
            Panic.panic(e);
        }

        flushToDisk();
    }

    // increase one to xid counter and update header of xid file
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        flushToDisk();
    }

    private void flushToDisk() {
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // start a new transaction
    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    // commit a transaction
    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // abort a transaction
    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    // check if xid transaction is in input `state`
    private boolean checkXID(long xid, byte state) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.allocate(XID_FIELD_SIZE);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == state;
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
