package com.weibo.dip.data.platform.datacubic.youku.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by yurun on 16/12/19.
 */
public class GetHour implements UDF1<String, String> {

    private String yesterday;

    public GetHour(String yesterday) {
        this.yesterday = yesterday;
    }

    @Override
    public String call(String logTime) throws Exception {
        if (StringUtils.isNotEmpty(logTime)) {
            String[] words = logTime.split(" ");

            if (words.length == 2) {
                String date = words[0];
                String time = words[1];

                if (date.equals(yesterday)) {
                    return time.substring(0, 2);
                }
            }
        }

        return null;
    }

}
