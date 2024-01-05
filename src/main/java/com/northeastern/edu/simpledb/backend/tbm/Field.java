package com.northeastern.edu.simpledb.backend.tbm;

import com.google.common.primitives.Bytes;
import com.northeastern.edu.simpledb.backend.im.BPlusTree;
import com.northeastern.edu.simpledb.backend.parser.statement.SingleExpression;
import com.northeastern.edu.simpledb.backend.utils.Panic;
import com.northeastern.edu.simpledb.backend.utils.ParseStringRes;
import com.northeastern.edu.simpledb.backend.utils.Parser;
import com.northeastern.edu.simpledb.common.Error;

import java.util.Arrays;
import java.util.List;

import static com.northeastern.edu.simpledb.backend.tm.TransactionManager.SUPER_XID;

public class Field {

    String fieldName;
    String fieldType;
    long index;
    long uid;
    private Table table;
    private BPlusTree bPlusTree;

    public Field(long uid, Table table) {
        this.uid = uid;
        this.table = table;
    }

    public Field(Table table, String fieldName, String fieldType, long index) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
        this.table = table;
    }

    // given table and uid, return a field
    public static Field loadField(Table table, long uid) {
        byte[] raw = null;
        try {
            raw = table.tbm.vm.read(SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, table).parseSelf(raw);
    }

    public static Field createField(Table table, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field field = new Field(table, fieldName, fieldType, 0);
        // if this field is an index, create a B+ Tree for it
        if (indexed) {
            long index = BPlusTree.create(table.tbm.dm);
            BPlusTree bpt = BPlusTree.load(index, table.tbm.dm);
            field.index = index;
            field.bPlusTree = bpt;
        }
        field.persistSelf(xid);
        return field;
    }

    public void insert(Object key, long uid) throws Exception {
        long uKey = value2UKey(key);
        bPlusTree.insert(uKey, uid);
    }

    /**
     * convert object to uid key used for searching or insertion of B+ Tree
     * eg: where id = 8, here 8 is input object key, and it will be parsed
     * into int32
      */
    private long value2UKey(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String) key);
                break;
            case "int32":
                int uint = (int) key;
                return (long) uint;
            case "int64":
                uid = (long) key;
                break;
        }
        return uid;
    }

    // convert object to a byte array for storing into disk
    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int) v);
                break;
            case "int64":
                raw = Parser.long2Byte((long) v);
                break;
            case "string":
                raw = Parser.string2Byte((String) v);
                break;
        }
        return raw;
    }

    // convert single expression into FieldCalRes
    public FieldCalRes calExp(SingleExpression exp) {
        Object v;
        FieldCalRes fieldCalRes = new FieldCalRes();
        switch (exp.compareOp) {
            case "<":
                fieldCalRes.left = 0;
                v = string2Value(exp.value);
                long right = value2UKey(v);
                fieldCalRes.right = right;
                if (fieldCalRes.right > 0) {
                    fieldCalRes.right --;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                fieldCalRes.left = value2UKey(v);
                fieldCalRes.right = fieldCalRes.left;
                break;
            case ">":
                fieldCalRes.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                fieldCalRes.left = value2UKey(v);
                break;
        }
        return fieldCalRes;
    }


    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes parseStringRes = null;
        parseStringRes = Parser.parseString(raw);
        position += parseStringRes.next;
        fieldName = parseStringRes.str;
        parseStringRes = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = parseStringRes.str;
        position += parseStringRes.next;
        index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + Long.BYTES));
        if (index != 0) {
            try {
                BPlusTree.load(index, table.tbm.dm);
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] fieldRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        this.uid = table.tbm.vm.insert(xid, Bytes.concat(nameRaw, fieldRaw, indexRaw));
    }

    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    public boolean isIndexed() {
        return index != 0;
    }

    public List<Long> search(long l, long r) throws Exception {
        return bPlusTree.searchRange(l, r);
    }

    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int) v);
                break;
            case "int64":
                str = String.valueOf((long) v);
                break;
            case "string":
                str = (String) v;
                break;
        }
        return str;
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    public ParseValueRes parseValue(byte[] raw) {
        ParseValueRes parseValueRes = new ParseValueRes();
        switch (fieldType) {
            case "int32":
                parseValueRes.v = Parser.parseInt(raw);
                parseValueRes.shift = Integer.BYTES;
                break;
            case "int64":
                parseValueRes.v = Parser.parseLong(raw);
                parseValueRes.shift = Long.BYTES;
                break;
            case "string":
                ParseStringRes parseStringRes = Parser.parseString(raw);
                parseValueRes.v = parseStringRes.str;
                parseValueRes.shift = parseStringRes.next;
                break;
        }
        return parseValueRes;
    }
}
