package com.weibo.dip.ml.godeyes;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by yurun on 17/7/12.
 */
public class TimePrint {

    public static void main(String[] args) throws Exception {
        Calendar time = Calendar.getInstance();

        time.setTimeInMillis(System.currentTimeMillis());

        System.out.println(time.get(Calendar.HOUR_OF_DAY));

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = dateFormat.parse("22017-07-13 12:03:52");

        System.out.println(dateFormat.format(date));

        System.out.println(date.getYear());

        System.out.println(dateFormat.format(new Date(Long.MAX_VALUE)));
    }

}
