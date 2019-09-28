package com.weibo.dip.data.platform.datacubic.delia.demo;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by xiaoyu on 2017/5/11.
 */
public class IOUtilsDemo {
    public static void readTest() throws IOException {
        try{
            byte[] bytes = new byte[4];
            InputStream is = IOUtils.toInputStream("hello world");
            IOUtils.read(is, bytes);
            System.out.println(new String(bytes));

            bytes = new byte[10];
            is = IOUtils.toInputStream("hello world");
            System.out.println(IOUtils.read(is, bytes, 2, 4));
            System.out.println(new String(bytes));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] mm) throws IOException {
        readTest();
    }
}
