package com.weibo.dip.data.platform.datacubic.streaming.udf.live;

import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by yurun on 17/2/17.
 */
public class GetLike extends ParseExtend implements UDF1<String, Long> {

    @Override
    public Long call(String extend) throws Exception {
        return Long.valueOf(parse(extend).getOrDefault("like_cnt", "0"));
    }

}
