package com.weibo.dip.data.platform.falcon.transport.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Wen on 2017/2/8.\
 */
public class DateUtils {

    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(ConfigUtils.TIME_PATTERN);

    /**
     * @param time Long
     * @return yyyyMMddhhmmss
     */
    public static String long2String(long time) {
        Date date = new Date(time);
        //        Calendar calendar = Calendar.getInstance();
        //        calendar.setTime(date);
        //        String hour = Integer.toString(calendar.get(Calendar.HOUR_OF_DAY));
        //        String minute = Integer.toString(calendar.get(Calendar.MINUTE));
        //        String second = Integer.toString(calendar.get(Calendar.SECOND));
        //        String day = Integer.toString(calendar.get(Calendar.DAY_OF_MONTH));
        //        String month = Integer.toString(calendar.get(Calendar.MONTH) + 1);
        //        String year = Integer.toString(calendar.get(Calendar.YEAR));
        //        return year+month+day+hour+minute+second;

        return SIMPLE_DATE_FORMAT.format(date);
    }

    public static long String2long(String time) throws ParseException {
        return SIMPLE_DATE_FORMAT.parse(time).getTime();
    }

    private DateUtils() {

    }

    public static void main(String[] args) throws ParseException {
        System.out.println(String2long("20170226105836"));
        System.out.println(String2long("20170309192800"));
    }
}
