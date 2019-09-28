package com.weibo.dip.data.platform.datacubic.test;

import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Created by yurun on 17/7/28.
 */
public class TimeFormatMain {

    public static void main(String[] args) throws Exception {
        String timeStr = "28/Jul/2017:17:18:49";

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", new Locale("US"));

        SimpleDateFormat consoleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println(consoleFormat.format(dateFormat.parse(timeStr)));
    }

}
