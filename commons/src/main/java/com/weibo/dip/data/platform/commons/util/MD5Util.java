package com.weibo.dip.data.platform.commons.util;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.Objects;

/**
 * Created by yurun on 17/7/12.
 */
public class MD5Util {

    private static final Logger LOGGER = LoggerFactory.getLogger(MD5Util.class);

    private static final String MD5 = "MD5";

    private static final char[] DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C',
        'D', 'E', 'F'};

    public static byte[] getBytes(String data) {
        if (Objects.isNull(data)) {
            return null;
        }

        try {
            MessageDigest digest = MessageDigest.getInstance(MD5);

            digest.update(data.getBytes(CharEncoding.UTF_8));

            return digest.digest();
        } catch (Exception e) {
            LOGGER.error("get md5 bytes error: {}", ExceptionUtils.getFullStackTrace(e));
        }

        return null;
    }

    public static String getString(String data) {
        byte[] bytes = getBytes(data);

        if (ArrayUtils.isEmpty(bytes)) {
            return null;
        }

        char[] md5 = new char[bytes.length * 2];

        for (int index = 0; index < bytes.length; index++) {
            md5[index * 2] = DIGITS[bytes[index] >>> 4 & 0xf];

            md5[index * 2 + 1] = DIGITS[bytes[index] & 0xf];
        }

        return new String(md5);
    }

    public static void main(String[] args) {
        String data = "hello world";

        byte[] bytes = getBytes(data);
        if (ArrayUtils.isNotEmpty(bytes)) {
            System.out.println(bytes.length);
        }

        System.out.println(getString(data));
    }

}
