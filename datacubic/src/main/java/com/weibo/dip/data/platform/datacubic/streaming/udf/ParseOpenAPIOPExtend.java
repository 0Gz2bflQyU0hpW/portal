package com.weibo.dip.data.platform.datacubic.streaming.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/4/11.
 */
public class ParseOpenAPIOPExtend implements UDF1<String, Map<String, String>> {

    private static final Pattern KEY_PATTERN = Pattern.compile("([^,|=|>]+)=>");

    @Override
    public Map<String, String> call(String line) throws Exception {
        if (StringUtils.isEmpty(line)) {
            return null;
        }

        Map<String, String> result = new HashMap<>();

        List<String> keys = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();

        Matcher matcher = KEY_PATTERN.matcher(line);

        while (matcher.find()) {
            keys.add(matcher.group(1));
            indices.add(matcher.start());
        }

        for (int index = 0; index < keys.size(); index++) {
            String key = keys.get(index);
            String value;

            int keyStart = indices.get(index);

            int valueStart = keyStart + key.length() + 2;
            int valueEnd = line.length();

            if (index < (keys.size() - 1)) {
                valueEnd = indices.get(index + 1) - 1;
            }

            value = line.substring(valueStart, valueEnd);

            result.put(key, value);
        }

        return result;
    }

}
