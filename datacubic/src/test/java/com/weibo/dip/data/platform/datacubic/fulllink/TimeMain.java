package com.weibo.dip.data.platform.datacubic.fulllink;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yurun on 17/3/29.
 */
public class TimeMain {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        System.out.println(DATE_FORMAT.format(new Date(1496764800000L)));
    }

}
