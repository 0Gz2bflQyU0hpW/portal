package com.weibo.dip.data.platform.datacubic.streaming.udf.live;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yurun on 17/2/15.
 */
public class ParseExtend {

    protected Map<String, String> parse(String extend) throws Exception {
        Map<String, String> datas = new HashMap<>();

        if (StringUtils.isEmpty(extend)) {
            return datas;
        }

        String[] pairs = extend.split(",");

        for (String pair : pairs) {
            String[] words = pair.split("=>");

            if (ArrayUtils.isEmpty(words) || words.length != 2) {
                continue;
            }

            datas.put(words[0], words[1]);
        }

        return datas;
    }

}
