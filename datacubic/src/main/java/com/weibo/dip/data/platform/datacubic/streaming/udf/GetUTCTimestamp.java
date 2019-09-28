package com.weibo.dip.data.platform.datacubic.streaming.udf;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.api.java.UDF2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by yurun on 16/12/19.
 */
public class GetUTCTimestamp implements UDF2<String, String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetUTCTimestamp.class);

    private SimpleDateFormat source = null;

    private SimpleDateFormat target = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    {
        target.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    public String call(String time, String format) throws Exception {
        if (source == null) {
            source = new SimpleDateFormat(format);
        }

        try {
            return target.format(source.parse(time));
        } catch (Exception e) {
            LOGGER.error("getUTCTimestamp error: " + ExceptionUtils.getFullStackTrace(e));

            return null;
        }
    }

}
