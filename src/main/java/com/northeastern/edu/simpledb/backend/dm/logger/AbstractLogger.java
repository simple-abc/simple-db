package com.northeastern.edu.simpledb.backend.dm.logger;

import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.backend.utils.Parser;
import com.northeastern.edu.simpledb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.northeastern.edu.simpledb.backend.dm.logger.Logger.LOG_SUFFIX;

abstract class AbstractLogger {
    abstract void log(byte[] data);
    abstract void truncate(long x) throws IOException;
    abstract byte[] next();
    abstract void rewind();
    abstract void close();

    // create log file based on path, initialize logger
    public static Logger create(String path) {
        File f = new File(path + LOG_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));

        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new Logger(raf, fc, 0);
    }

    // open log file based on path, init, check and format logger
    public static Logger open(String path) {
        File f = new File(path + LOG_SUFFIX);

        if (!f.exists()) Panic.panic(Error.FileNotExistsException);
        if (!f.canRead() || !f.canWrite()) Panic.panic(Error.FileCannotRWException);

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        Logger logger = new Logger(raf, fc);
        logger.init();

        return logger;
    }
}
