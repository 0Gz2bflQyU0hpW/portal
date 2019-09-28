package com.weibo.dip.data.platform.datacubic.streaming.udf.videotrace.ha;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by yurun on 17/1/19.
 */
public class GetIdc implements UDF1<String, String> {

    @Override
    public String call(String idc) throws Exception {
        return StringUtils.isEmpty(idc) ? "Other" : idc;
    }

}
