package com.weibo.dip.data.platform.datacubic.streaming.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by yurun on 17/3/29.
 */
public class TimeToUTC implements UDF1<Long, String> {

    private SimpleDateFormat target = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    {
        target.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    public String call(Long time) throws Exception {
        return target.format(new Date(time));
    }

}
