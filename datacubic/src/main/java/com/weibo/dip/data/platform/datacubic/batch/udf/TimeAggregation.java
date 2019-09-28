package com.weibo.dip.data.platform.datacubic.batch.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yurun on 17/2/23.
 */

public class TimeAggregation implements UDF1<Timestamp, String> {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public String call(Timestamp time) throws Exception {
        return dateFormat.format(new Date(time.getTime()));
    }

}
