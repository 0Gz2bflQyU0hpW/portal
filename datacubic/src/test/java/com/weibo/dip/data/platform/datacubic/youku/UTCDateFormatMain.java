package com.weibo.dip.data.platform.datacubic.youku;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yurun on 16/12/29.
 */
public class UTCDateFormatMain {

    public static void main(String[] args) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

        //df.setTimeZone(TimeZone.getTimeZone("UTC"));

        System.out.println(df.format(new Date()));
    }

}
