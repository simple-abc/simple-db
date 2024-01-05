package com.northeastern.edu.simpledb.backend.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Parser {

    public static long parseLong(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, 0, Long.SIZE);
        return buf.getLong();
    }

    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static short parseShort(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, 0, Short.BYTES);
        return buf.getShort();
    }

    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] raw) {
        return ByteBuffer.wrap(raw, 0, Integer.SIZE).getInt();
    }

    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, Integer.BYTES));
        String str = new String(Arrays.copyOfRange(raw, Integer.BYTES, Integer.BYTES + length));
        return new ParseStringRes(str, length + Integer.BYTES);
    }

    public static byte[] string2Byte(String str) {
        byte[] length = int2Byte(str.length());
        return Bytes.concat(length, str.getBytes(StandardCharsets.UTF_8));
    }

    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }
}
