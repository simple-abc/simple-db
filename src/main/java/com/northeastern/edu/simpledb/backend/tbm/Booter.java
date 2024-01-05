package com.northeastern.edu.simpledb.backend.tbm;

import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.common.Error;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class Booter {

    private static final String BOOTER_SUFFIX = ".bt";
    private static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    public Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    public static Booter open(String path) {
        removeBadTmp(path);
        File tmp = new File(path + BOOTER_SUFFIX);
        if (!tmp.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!tmp.canWrite() || !tmp.canRead()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, tmp);
    }

    public static Booter create(String path) {
        removeBadTmp(path);
        File tmp = new File(path + BOOTER_SUFFIX);
        try {
            if (!tmp.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, tmp);
    }

    private static void removeBadTmp(String path) {
        new File(path + BOOTER_TMP_SUFFIX).delete();
    }

    public void update(byte[] data) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            Files.move(tmp.toPath(), new File(path + BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Panic.panic(e);
        }

        file = new File(path + BOOTER_SUFFIX);
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
    }

    public byte[] load() {
        byte[] bytes = null;
        try {
            bytes = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return bytes;
    }


}
