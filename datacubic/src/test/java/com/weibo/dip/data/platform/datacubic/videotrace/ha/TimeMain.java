package com.weibo.dip.data.platform.datacubic.videotrace.ha;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by yurun on 17/1/19.
 */
public class TimeMain {

    public static void main(String[] args) throws Exception {
        SimpleDateFormat target = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        target.setTimeZone(TimeZone.getTimeZone("UTC"));

        String formatStr = target.format(new Date());

        System.out.println(formatStr);
        System.out.println("2017-02-06T16:14:00.000Z");

        Date date = target.parse(formatStr);

        System.out.println(date);

        date = target.parse("2017-02-06T16:14:00.000Z");

        System.out.println(date);
    }

}
