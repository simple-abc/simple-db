package com.northeastern.edu.simpledb.backend.utils;

import java.nio.ByteBuffer;

public class Parser {

    public static long parseLong(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, 0, 8);
        return buf.getLong();
    }

    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static short parseShort(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, 0, 2);
        return buf.getShort();
    }

    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] array) {
        ByteBuffer buf = ByteBuffer.wrap(array, 0, array.length);
        return buf.getInt();
    }
}
