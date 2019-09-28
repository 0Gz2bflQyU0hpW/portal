package com.weibo.dip.data.platform.datacubic.streaming.udf.videotrace.ha;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by yurun on 17/1/19.
 */
public class GetDomain implements UDF1<String, String> {

    @Override
    public String call(String captured_request_headers) throws Exception {
        return StringUtils.isEmpty(captured_request_headers) ? "Other" : captured_request_headers;
    }

}
