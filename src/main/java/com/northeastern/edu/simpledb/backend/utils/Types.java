package com.northeastern.edu.simpledb.backend.utils;

public class Types {
    public static long addressToUid(int pageNumber, short offset) {
        long u0 = pageNumber;
        long u1 = offset;
        return u0 << 32 | u1;
    }
}
