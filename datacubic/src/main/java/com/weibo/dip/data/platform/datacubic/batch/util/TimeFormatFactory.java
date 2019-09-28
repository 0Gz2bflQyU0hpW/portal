package com.weibo.dip.data.platform.datacubic.batch.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by xiaoyu on 2017/5/10.
 */
public class TimeFormatFactory {
    private static String YYYYMMDD_Str = null;
    private static String YYYY_MM_DD_Str = null;

    private static SimpleDateFormat YYYYMMDD = new SimpleDateFormat("yyyyMMdd");
    private static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");

    private TimeFormatFactory(){}

    public static String getYYYYMMDD(Date date){
        if(YYYYMMDD_Str == null){
            synchronized (TimeFormatFactory.class){
                if (YYYYMMDD_Str == null){
                    YYYYMMDD_Str = YYYYMMDD.format(date);
                }
            }
        }

        return YYYYMMDD_Str;
    }

    public static String getYesterDay(Date date){
        if(YYYY_MM_DD_Str == null){
            synchronized (TimeFormatFactory.class){
                if (YYYY_MM_DD_Str == null){
                    Calendar cal = Calendar.getInstance();

                    cal.setTime(date);
                    cal.add(Calendar.DAY_OF_MONTH, -1);

                    YYYY_MM_DD_Str = YYYY_MM_DD.format(cal.getTime());
                }
            }
        }
        return YYYY_MM_DD_Str;
    }




}
