package com.northeastern.edu.simpledb.backend.utils;

import java.security.SecureRandom;

public class RandomUtil {

    public static byte[] randomBytes(int length) {
        SecureRandom random = new SecureRandom();
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

}
