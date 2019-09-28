package com.weibo.dip.data.platform.datacubic.test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by yurun on 17/5/18.
 */
public class SplitMain {

    public static void main(String[] args) {
        Calendar c = Calendar.getInstance();

        c.setTime(new Date());

        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        System.out.println(dateFormat.format(c.getTime()));
    }

}
