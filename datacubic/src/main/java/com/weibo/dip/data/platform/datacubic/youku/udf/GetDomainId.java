package com.weibo.dip.data.platform.datacubic.youku.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by yurun on 16/12/19.
 */
public class GetDomainId implements UDF1<String, String> {

    @Override
    public String call(String oid) throws Exception {
        if (StringUtils.isNotEmpty(oid)) {
            String[] words = oid.split(":");
            if (words.length > 1) {
                return words[0];
            }
        }

        return null;
    }

}
