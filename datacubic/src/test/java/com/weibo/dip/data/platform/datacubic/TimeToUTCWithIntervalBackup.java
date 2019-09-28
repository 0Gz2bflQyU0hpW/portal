package com.weibo.dip.data.platform.datacubic;

import org.apache.spark.sql.api.java.UDF2;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by yurun on 17/3/29.
 */
public class TimeToUTCWithIntervalBackup implements UDF2<Long, String, String> {

    private static final String YEAR = "year";
    private static final String MONTH = "month";
    private static final String DAY = "day";
    private static final String HOUR = "hour";
    private static final String MINUTE = "minute";
    private static final String SECOND = "second";
    private static final String MILL = "mill";

    private SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy-01-01'T'00:00:00.000'Z'");
    private SimpleDateFormat monthFormat = new SimpleDateFormat("yyyy-MM-01'T'00:00:00.000'Z'");
    private SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd'T'00:00:00.000'Z'");
    private SimpleDateFormat hourFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00.000'Z'");
    private SimpleDateFormat minuteFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:00.000'Z'");
    private SimpleDateFormat secondFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.000'Z'");
    private SimpleDateFormat millFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    {
        yearFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        secondFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        secondFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        secondFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        secondFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        secondFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        secondFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    public String call(Long time, String interval) throws Exception {
        switch (interval) {
            case YEAR:
                return yearFormat.format(new Date(time));

            case MONTH:
                return monthFormat.format(new Date(time));

            case DAY:
                return dayFormat.format(new Date(time));

            case HOUR:
                return hourFormat.format(new Date(time));

            case MINUTE:
                return minuteFormat.format(new Date(time));

            case SECOND:
                return secondFormat.format(new Date(time));

            case MILL:
                return millFormat.format(new Date(time));

            default:
                return millFormat.format(new Date(time));
        }
    }

    public static void main(String[] args) throws Exception {
        TimeToUTCWithIntervalBackup timeToUTCWithInterval = new TimeToUTCWithIntervalBackup();

        String result = timeToUTCWithInterval.call(System.currentTimeMillis(), "month");

        System.out.println(result);
    }

}
