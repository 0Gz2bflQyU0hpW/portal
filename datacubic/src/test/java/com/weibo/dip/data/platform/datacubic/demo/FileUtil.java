package com.weibo.dip.data.platform.datacubic.demo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created by delia on 2017/2/23.
 */
public class FileUtil {
    public static BufferedReader getFileReader(String path) throws FileNotFoundException {
        return new BufferedReader(new FileReader(path));
    }
}
