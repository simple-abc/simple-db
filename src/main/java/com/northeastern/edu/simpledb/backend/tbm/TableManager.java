package com.northeastern.edu.simpledb.backend.tbm;

import com.northeastern.edu.simpledb.backend.dm.DataManger;
import com.northeastern.edu.simpledb.backend.parser.statement.*;
import com.northeastern.edu.simpledb.backend.utils.Parser;
import com.northeastern.edu.simpledb.backend.vm.VersionManager;
import com.northeastern.edu.simpledb.common.Error;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TableManager extends AbstractTableManager{

    private Booter booter;
    private Map<String, Table> tableCache;
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;

    public TableManager(VersionManager vm, DataManger dm, Booter booter) {
        this.booter = booter;
        this.vm = vm;
        this.dm = dm;
        this.tableCache = new ConcurrentHashMap<>();
        this.xidTableCache = new ConcurrentHashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    private void loadTables() {
        long uid = firstTableUid();
        while (uid != 0) {
            Table table = Table.loadTable(this, uid);
            uid = table.nextUid;
            tableCache.put(table.name, table);
        }
    }

    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    @Override
    public BeginRes begin(Begin begin) {
        BeginRes beginRes = new BeginRes();
        int isolationLevel = begin.isRepeatableRead ? 1 : 0;
        beginRes.xid = vm.begin(isolationLevel);
        beginRes.result = "begin".getBytes();
        return beginRes;
    }

    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            if(t == null) {
                return "\n".getBytes();
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if (tableCache.containsKey(create.tableName)) throw Error.DuplicatedTableException;
            Table table = Table.createTable(this, firstTableUid(), xid, create); // How does it work?
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            xidTableCache.computeIfAbsent(xid, k -> new ArrayList<>()).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        Table table = tableCache.get(insert.tableName);
        if (table == null) throw Error.TableNotFoundException;
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    @Override
    public byte[] read(long xid, Select read) throws Exception {
        Table table = tableCache.get(read.tableName);
        if (table == null) throw Error.TableNotFoundException;
        return table.read(xid, read).getBytes();
    }

    @Override
    public byte[] update(long xid, Update update) throws Exception {
        Table table = tableCache.get(update.tableName);
        if(table == null) throw Error.TableNotFoundException;
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        Table table = tableCache.get(delete.tableName);
        if(table == null) throw Error.TableNotFoundException;
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }
}
