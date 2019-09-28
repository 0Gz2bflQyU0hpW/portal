package com.weibo.dip.data.platform.datacubic.streaming.udf.videotrace.ha;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 17/1/19.
 */
public class GetSize implements UDF1<String, Long> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetSize.class);

    @Override
    public Long call(String bytesRead) throws Exception {
        try {
            return Long.valueOf(bytesRead);
        } catch (NumberFormatException e) {
            LOGGER.error("getSize error: " + ExceptionUtils.getFullStackTrace(e));

            return 0L;
        }
    }

}
