package com.northeastern.edu.simpledb.backend.tbm;

import com.google.common.primitives.Bytes;
import com.northeastern.edu.simpledb.backend.parser.statement.*;
import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.backend.utils.ParseStringRes;
import com.northeastern.edu.simpledb.backend.utils.Parser;
import com.northeastern.edu.simpledb.common.Error;

import java.util.*;

import static com.northeastern.edu.simpledb.backend.tm.TransactionManager.SUPER_XID;

public class Table {

    TableManager tbm;
    long uid;
    long nextUid;
    String name;
    byte state;
    List<Field> fields = new ArrayList<>();

    public Table(TableManager tableManager, long uid) {
        this.tbm = tableManager;
        this.uid = uid;
    }

    public Table(TableManager tableManager, String tableName, long nextUid) {
        this.tbm = tableManager;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    public static Table loadTable(TableManager tableManager, long uid) {
        byte[] raw = null;
        try {
            raw = tableManager.vm.read(SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table table = new Table(tableManager, uid);
        return table.parseSelf(raw);
    }

    public static Table createTable(TableManager tableManager, long nextUid, long xid, Create create) throws Exception {
        Table table = new Table(tableManager, create.tableName, nextUid);
        for (int i = 0; i < create.fieldName.length; i++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = Arrays.asList(create.index).contains(fieldName);
            table.fields.add(Field.createField(table, xid, fieldName, fieldType, indexed));
        }
        return table.persistSelf(xid);
    }

    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        // insert into page
        long uid = tbm.vm.insert(xid, raw);
        // insert into index
        for (Field field : fields) {
            if (field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    private Map<String, Object> string2Entry(String[] values) {
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            Object v = field.string2Value(values[i]);
            entry.put(field.fieldName, v);
        }
        return entry;
    }

    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if (tbm.vm.delete(xid, uid)) {
                count++;
            }
        }
        return count;
    }

    // return all uid in the range
    private List<Long> parseWhere(Where where) throws Exception {
        long l0 = 0L, r0 = 0L, l1 = 0L, r1 = 0L;
        Boolean single = false;
        Field fd = null;
        if (where == null) { // condition is empty, use the first index in fields
            for (Field field : fields) {
                if (field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else { // condition is not empty, try to use index in the field
            for (Field field : fields) {
                if (field.fieldName.equals(where.singleExp1.field)) {
                    if (!field.isIndexed()) throw Error.FieldNotIndexedException;
                }
                fd = field;
                break;
            }
            if (fd == null) throw Error.FieldNotIndexedException;
            CalWhereRes calWhereRes = calWhere(fd, where);
            l0 = calWhereRes.l0; r0 = calWhereRes.r0;
            l1 = calWhereRes.l1; r1 = calWhereRes.r1;
            single = calWhereRes.single;
        }
        List<Long> uids = fd.search(l0, r0);
        if (!single) {
            List<Long> list = fd.search(l1, r1);
            uids.addAll(list);
        }
        return uids;
    }

    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes calWhereRes = new CalWhereRes();
        FieldCalRes fieldCalRes = null;
        switch (where.logicOp) {
            case "":
                calWhereRes.single = true;
                fieldCalRes = fd.calExp(where.singleExp1);
                calWhereRes.l0 = fieldCalRes.left;
                calWhereRes.r0 = fieldCalRes.right;
                break;
            case "or":
                calWhereRes.single = false;
                fieldCalRes = fd.calExp(where.singleExp1);
                calWhereRes.l0 = fieldCalRes.left;
                calWhereRes.r0 = fieldCalRes.right;
                fieldCalRes = fd.calExp(where.singleExp2);
                calWhereRes.l1 = fieldCalRes.left;
                calWhereRes.r1 = fieldCalRes.right;
                break;
            case "and":
                calWhereRes.single = false;
                fieldCalRes = fd.calExp(where.singleExp1);
                calWhereRes.l0 = fieldCalRes.left;
                calWhereRes.r0 = fieldCalRes.right;
                fieldCalRes = fd.calExp(where.singleExp2);
                calWhereRes.l1 = fieldCalRes.left;
                calWhereRes.r1 = fieldCalRes.right;
                if (calWhereRes.l1 > calWhereRes.l0) calWhereRes.l0 = calWhereRes.l1;
                if (calWhereRes.r1 < calWhereRes.r0) calWhereRes.r0 = calWhereRes.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return calWhereRes;
    }

    private class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    public int update(long xid, Update update) throws Exception {
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        for (Field f : fields) {
            if (f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if (fd == null) throw Error.FieldNotFoundException;
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            byte[] raw = tbm.vm.read(xid, uid);
            if (raw == null) continue;
            tbm.vm.delete(xid, uid);
            Map<String, Object> entry = parseEntry(raw);
            entry.put(fd.fieldName, value);
            raw = entry2Raw(entry);
            long uuid = tbm.vm.insert(xid, raw);

            count ++;

            for (Field field : fields) {
                if (field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            Field.ParseValueRes parseValueRes = field.parseValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, parseValueRes.v);
            pos += parseValueRes.shift;
        }
        return entry;
    }

    public String read(long xid, Select read) throws Exception {
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            byte[] raw = tbm.vm.read(xid, uid);
            if (raw == null) continue;
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry, read)).append("\n");
        }
        return sb.toString();
    }

    private String printEntry(Map<String, Object> entry, Select select) {
        StringBuilder sb = new StringBuilder("[");
        int n = select.fields.length;
        List<Field> printField = new ArrayList<>();
        if (n > 0 && select.fields[0].equals("*")) {
            printField = fields;
        } else {
            for (int i = 0; i < n; i++) {
                for(Field field : fields) {
                    if (field.fieldName.equals(select.fields[i])) {
                        printField.add(field);
                        break;
                    }
                }
            }
        }

        for (int i = 0; i < printField.size(); i++) {
            Field field = printField.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if(i == printField.size() - 1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if(i == fields.size() - 1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for (Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        this.uid = tbm.vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes parseStringRes = Parser.parseString(raw);
        name = parseStringRes.str;
        position += parseStringRes.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + Long.BYTES));
        position += Long.SIZE;

        while (position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + Long.BYTES));
            position += Long.SIZE;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for (Field field :
                fields) {
            sb.append(field.toString());
            if (field == fields.get(fields.size() - 1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

}
