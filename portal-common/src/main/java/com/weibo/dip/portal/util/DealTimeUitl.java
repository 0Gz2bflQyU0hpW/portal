package com.weibo.dip.portal.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by ftx on 2016/11/30.
 */
public class DealTimeUitl {

    /**
     * 求输入时间，跨度几天后的时间。
     * @param date
     * @param span 整数（过去几天），负数（未来几天）
     * @return
     */
    public static Date transformDate(Date date, Long span) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DATE, (int) (calendar.get(Calendar.DATE) - span));
        return calendar.getTime();
    }

    /**
     * 以格式yyyy-MM-dd HH:mm:ss 输出当前时间。
     * @return
     */
    public static String getCurrentTimeFormat(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return  sdf.format(new Date());
    }
}
