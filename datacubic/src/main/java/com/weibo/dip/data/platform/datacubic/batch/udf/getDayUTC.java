package com.weibo.dip.data.platform.datacubic.batch.udf;

import com.weibo.dip.data.platform.datacubic.streaming.udf.GetUTCTimestamp;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Created by xiaoyu on 2017/6/8.
 */
public class getDayUTC implements UDF1<Long,String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(getDayUTC.class);

    private SimpleDateFormat target = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private Calendar calendar = Calendar.getInstance();


    {
        target.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    public String call(Long time) throws Exception {
        try {
            calendar.setTime(new Date(time));
            calendar.set(Calendar.HOUR_OF_DAY,0);
            calendar.set(Calendar.MINUTE,0);
            calendar.set(Calendar.SECOND,0);
            calendar.set(Calendar.MILLISECOND,0);

            return target.format(calendar.getTime());
        } catch (Exception e) {
            LOGGER.error("getUTCTimestamp error: " + ExceptionUtils.getFullStackTrace(e));

            return null;
        }
    }
}
