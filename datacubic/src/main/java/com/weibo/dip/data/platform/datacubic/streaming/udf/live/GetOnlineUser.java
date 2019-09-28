package com.weibo.dip.data.platform.datacubic.streaming.udf.live;

import org.apache.spark.sql.api.java.UDF2;

/**
 * Created by yurun on 17/2/17.
 */
public class GetOnlineUser extends ParseExtend implements UDF2<String, String, Long> {

    @Override
    public Long call(String action, String extend) throws Exception {
        switch (action) {
            case Action.JOIN_ROOM:
                return 1L;

            case Action.EXIT_ROOM:
                return -1L;

            default:
                return 0L;
        }
    }

}
