package com.weibo.dip.data.platform.datacubic.streaming.udf;

import org.apache.spark.sql.api.java.UDF2;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yurun on 17/2/23.
 */
public class TimeAggregation implements UDF2<Long, Integer, String> {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public String call(Long time, Integer interval) throws Exception {
        return dateFormat.format(new Date(interval * (time / interval)));
    }

}
