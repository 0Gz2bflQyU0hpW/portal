package com.weibo.dip.data.platform.datacubic.demo;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by xiaoyu on 2017/3/1.
 */
public class DateFormatDemo {
    private static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");
    private static final String HDFS_INPUT = "/user/hdfs/rawlog/app_weibomobile03x4ts1kl_clientperformance";
    private static final String HDFS_OUTPUT = "/user/hdfs/tmp/fulllink";

    public static String getInputPath(Date today, int hour) {
        return HDFS_INPUT + "/" + YYYY_MM_DD.format(today) + "/" + String.format("%02d", hour) + "/*";
    }

    public static String getOutputPath(Date today, int hour) {
        return HDFS_OUTPUT + "/" + YYYY_MM_DD.format(today) + "/" + String.format("%02d", hour);
    }

    public static String getFormattedDate(Date date) {
        return YYYY_MM_DD.format(date);
    }

    public static void main(String[] args) {
//        System.out.println(getInputPathYesterday(new Date(), 14));
//        System.out.println(getOutputPath(new Date(), 14));
//        System.out.println(YYYY_MM_DD.);
    }
}
