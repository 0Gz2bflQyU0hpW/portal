package com.weibo.dip.data.platform.datacubic.streaming.udf;

import org.apache.spark.sql.api.java.UDF2;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by yurun on 17/3/29.
 */
public class TimeToUTCWithInterval implements UDF2<Long, String, String> {
    private static final String YEAR = "year";
    private static final String MONTH = "month";
    private static final String DAY = "day";
    private static final String HOUR = "hour";
    private static final String MINUTE = "minute";
    private static final String FIVE_MINUTE = "five_minutes";
    private static final String SECOND = "second";
    private static final String MILL = "mill";

    private Calendar calendar = Calendar.getInstance();
    private SimpleDateFormat formatUtc = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    {
        formatUtc.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private Date getDate(Long time, String interval) {
        calendar.setTimeInMillis(time);

        switch (interval) {
            case FIVE_MINUTE:
                int min5 = 5 * (calendar.get(Calendar.MINUTE) / 5);  // get five - minut scale

                calendar.set(Calendar.MINUTE, min5);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);

                return calendar.getTime();

            case MILL:
                return calendar.getTime();

            case YEAR:
                calendar.set(Calendar.MONTH, 0);

            case MONTH:
                calendar.set(Calendar.DAY_OF_MONTH, 1);

            case DAY:
                calendar.set(Calendar.HOUR_OF_DAY, 0);

            case HOUR:
                calendar.set(Calendar.MINUTE, 0);

            case MINUTE:
                calendar.set(Calendar.SECOND, 0);

            case SECOND:
                calendar.set(Calendar.MILLISECOND, 0);

                return calendar.getTime();

            default:
                String message = String.format("udf time interval: '%s' invalid. Like [year/month/day/hour/minute/five_minutes/second/mill]", interval);
                throw new RuntimeException(message);
        }

    }

    @Override
    public String call(Long time, String interval) {
        return formatUtc.format(getDate(time, interval));
    }

    public static void main(String[] args) throws Exception {
        TimeToUTCWithInterval timeToUTCWithInterval = new TimeToUTCWithInterval();

        long now = System.currentTimeMillis();

        String resultYear = timeToUTCWithInterval.call(now, "year");
        String resultMonth = timeToUTCWithInterval.call(now, "month");
        String resultDay = timeToUTCWithInterval.call(now, "day");
        String resultHour = timeToUTCWithInterval.call(now, "hour");
        String resultMinute = timeToUTCWithInterval.call(now, "minute");
        String result5Minute = timeToUTCWithInterval.call(now, "five_minutes");
        String resultSecond = timeToUTCWithInterval.call(now, "second");
        String resultMill = timeToUTCWithInterval.call(now, "mill");

        System.out.println("year         " + resultYear);
        System.out.println("month        " + resultMonth);
        System.out.println("day          " + resultDay);
        System.out.println("hour         " + resultHour);
        System.out.println("minute       " + resultMinute);
        System.out.println("five_minutes " + result5Minute);
        System.out.println("second       " + resultSecond);
        System.out.println("mill         " + resultMill);

        String resultError = timeToUTCWithInterval.call(now, "22");

        System.out.println(resultError);

    }

}

