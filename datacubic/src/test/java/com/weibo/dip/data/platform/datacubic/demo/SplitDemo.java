package com.weibo.dip.data.platform.datacubic.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by delia on 2017/2/27.
 */
public class SplitDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitDemo.class);

    public static void main(String[] args) {
//        String line = "LGE-Nexus 5__weibo__5.5.0__android__android5.0.1";
        String line = "";

        String allParams[] = line.split("__", 7);
        System.out.println(allParams.length + " :" + Arrays.toString(allParams));
    }

    public static String[] getParams(String line, String regex, int len) {
        String array[] = line.split(regex, len + 1);
        System.out.println("{"+array.length+":"+Arrays.toString(array)+"}");
        return array.length == len ? array : null;
    }

}

