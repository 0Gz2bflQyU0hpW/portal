package com.weibo.dip.databus.kafka.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormat {
    private DateFormat(){}

    /**
     * format: yyyy_MM_dd
     * @return
     */
    public static String getCurrentDate(Date date){
        java.text.DateFormat format = new SimpleDateFormat("yyyy_MM_dd");
        return format.format(date);
    }

    /**
     * format: HH
     * @return
     */
    public static String getCurrentHour(Date date){
        java.text.DateFormat format = new SimpleDateFormat("HH");
        return format.format(date);
    }

    /**
     * format: yyyyMMddHH
     * @return
     */
    public static String getCurrentDateHour(Date date){
        java.text.DateFormat format = new SimpleDateFormat("yyyyMMddHH");
        return format.format(date);
    }

    /**
     * format: HH_mm_ss
     * @return
     */
    public static String getCurrentTime(Date date){
        java.text.DateFormat format = new SimpleDateFormat("HH_mm_ss");
        return format.format(date);
    }



    public static void main(String[] args) {
        Date date = new Date();
        System.out.println(DateFormat.getCurrentDate(date));
        System.out.println(DateFormat.getCurrentHour(date));
        System.out.println(DateFormat.getCurrentDateHour(date));
        System.out.println(DateFormat.getCurrentTime(date));

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(DateFormat.getCurrentTime(date));
    }
}
