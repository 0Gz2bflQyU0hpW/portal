package com.weibo.dip.data.platform.datacubic.youku.udf;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by yurun on 16/12/19.
 */
public class GetVideoId implements UDF1<String, String> {

    @Override
    public String call(String extstr) throws Exception {
        if (StringUtils.isNotEmpty(extstr)) {
            String[] words = extstr.split("\\|");

            for (String word : words) {
                String[] pair = word.split(":");

                if (ArrayUtils.isEmpty(pair) || pair.length < 2) {
                    continue;
                }

                if (pair[0].equals("video_url")) {
                    return pair[1];
                }
            }
        }

        return null;
    }

}
