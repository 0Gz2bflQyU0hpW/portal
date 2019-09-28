package com.weibo.dip.data.platform.datacubic.streaming.udf.fulllink;

import org.apache.spark.sql.api.java.UDF2;

/**
 * Created by yurun on 17/3/30.
 */
public class TimeDistribution implements UDF2<Long, Long, Long> {

    @Override
    public Long call(Long time, Long aLong2) throws Exception {
        return null;
    }

}

