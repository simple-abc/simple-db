package com.northeastern.edu.simpledb.backend.tbm;

import com.northeastern.edu.simpledb.backend.dm.DataManger;
import com.northeastern.edu.simpledb.backend.parser.statement.*;
import com.northeastern.edu.simpledb.backend.utils.Parser;
import com.northeastern.edu.simpledb.backend.vm.VersionManager;

public abstract class AbstractTableManager {
    VersionManager vm;
    DataManger dm;
    public abstract BeginRes begin(Begin begin);
    public abstract byte[] commit(long xid) throws Exception;
    public abstract byte[] abort(long xid);
    public abstract byte[] show(long xid);
    public abstract byte[] create(long xid, Create create) throws Exception;
    public abstract byte[] insert(long xid, Insert insert) throws Exception;
    public abstract byte[] read(long xid, Select select) throws Exception;
    public abstract byte[] update(long xid, Update update) throws Exception;
    public abstract byte[] delete(long xid, Delete delete) throws Exception;

    public static AbstractTableManager create(String path, VersionManager vm, DataManger dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManager(vm, dm, booter);
    }

    public static AbstractTableManager open(String path, VersionManager vm, DataManger dm) {
        Booter booter = Booter.open(path);
        return new TableManager(vm, dm ,booter);
    }

}
