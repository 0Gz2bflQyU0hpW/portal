package com.weibo.dip.data.platform.datacubic.youku.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by yurun on 16/12/19.
 */
public class GetPlatform implements UDF1<String, String> {

    @Override
    public String call(String fromcode) throws Exception {
        if (StringUtils.isNotEmpty(fromcode)) {
            int len = fromcode.length();
            if (len >= 4) {
                String code = fromcode.substring(len - 4, len - 1);
                if (code.equals("301")) {
                    return "ios";
                } else if (code.equals("501")) {
                    return "android";

                }
            }
        }

        return null;
    }

}
