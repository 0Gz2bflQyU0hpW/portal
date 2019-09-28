package com.weibo.dip.data.platform.datacubic.fulllink;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yurun on 17/5/10.
 */
public class Time {

    public static void main(String[] args) {
        long now = System.currentTimeMillis();

        long interval = 24 * 3600 * 1000;

        long time = interval * (now / interval);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println(format.format(new Date(time)));

        System.out.println(Long.MAX_VALUE);
    }

}
