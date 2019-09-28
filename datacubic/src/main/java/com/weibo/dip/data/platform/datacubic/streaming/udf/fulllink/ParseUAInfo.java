package com.weibo.dip.data.platform.datacubic.streaming.udf.fulllink;

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by xiaoyu on 2017/2/24.
 */
public class ParseUAInfo implements UDF1<String, Map<String, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParseUAInfo.class);

    private static final String[] mapKeys = {"mobile_manufacturer", "mobile_model", "app", "app_version",
        "system", "system_version"};

    private static final String IPHONE = "iphone";
    private static final String ANDROID = "android";
    private static final String IPAD = "ipad";

    private static final String HYPHEN = "-";
    private static final String COMMA = ",";

    @Override
    public Map<String, String> call(String line) throws Exception {
        if (Objects.isNull(line)) {
            LOGGER.debug("line is null");

            return null;
        }

        String allParams[] = line.split("__", -1);
        if (allParams.length != mapKeys.length - 1) {
            LOGGER.debug("Parse line " + line + " with split error: inconsistent length");

            return null;
        }

        String delimiter;

        switch (allParams[3]) {
            case IPHONE:
            case IPAD:
                delimiter = COMMA;
                break;

            case ANDROID:
                delimiter = HYPHEN;
                break;

            default:
                delimiter = null;
        }

        String mobile_manufacturer;
        String mobile_model;

        if (Objects.isNull(delimiter)) {
            mobile_manufacturer = allParams[0];
            mobile_model = "NULL";
        } else {
            int separator = allParams[0].indexOf(delimiter);

            if (separator == -1) {
                mobile_manufacturer = allParams[0];
                mobile_model = "NULL";
            } else {
                mobile_manufacturer = allParams[0].substring(0, separator);
                mobile_model = allParams[0].substring(separator + 1);
            }
        }

        List<String> params = new ArrayList<>();

        params.add(mobile_manufacturer);
        params.add(mobile_model);

        params.addAll(Arrays.asList(Arrays.copyOfRange(allParams, 1, allParams.length)));

        Map<String, String> result = new HashMap<>();

        for (int index = 0; index < mapKeys.length; index++) {
            result.put(mapKeys[index], params.get(index));
        }

        return result;
    }

    public static void main(String[] args) throws Exception {
        ParseUAInfo parseUAInfo = new ParseUAInfo();

        String line = "iPhone9,2__weibo__7.5.0__iphone__os10.3.1";

        Map<String, String> result = parseUAInfo.call(line);

        System.out.println(result);

        line = "vivo-vivo X6SPlus D__weibo__7.3.1__android__android5.1.1";

        result = parseUAInfo.call(line);

        System.out.println(result);
    }

}
