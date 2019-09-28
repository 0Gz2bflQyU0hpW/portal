package com.weibo.dip.data.platform.datacubic.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by delia on 2017/2/24.
 */
public class RegexDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegexDemo.class);

    private static Pattern pattern = Pattern.compile("([\\s\\S]*)-([\\s\\S]*)__([\\s\\S]*)__([\\s\\S]*)__([\\s\\S]*)__([\\s\\S]*)");
    private static Pattern pattern1 = Pattern.compile("([^-]*)-([\\s\\S]*)__([\\s\\S]*)__([\\s\\S]*)__([\\s\\S]*)__([\\s\\S]*)");
    private static final String[] mapKeys = {"mobile_manufacturer", "mobile_model", "weibo", "weibo_version", "system", "system_version"};

    public static void main(String[] args) throws Exception {
        String line = "";
        line.split("");
        Matcher matcher = pattern.matcher(line);
        Map<String, String> result = null;
        if (matcher.matches()) {
            int groups = matcher.groupCount();

            if (groups == mapKeys.length) {
                result = new HashMap<>();
                for (int index = 0; index < groups; index++) {
                    result.put(mapKeys[index], matcher.group(index + 1));
                }
            } else {
                System.out.println("error");
            }
        } else {
            System.out.println("error");

        }
    }

    public static Map<String, String> getInfo(String line) throws Exception {
        Matcher matcher = pattern.matcher(line);
        Map<String, String> result = null;
        if (matcher.matches()) {
            int groups = matcher.groupCount();

            if (groups == mapKeys.length) {
                result = new HashMap<>();
                for (int index = 0; index < groups; index++) {
                    result.put(mapKeys[index], matcher.group(index + 1));
                }
            } else {
                LOGGER.debug("Parse line " + line + " with regex error: inconsistent groups");
            }
        } else {
            LOGGER.debug("Parse line " + line + " with regex error: regular expression mismatch");
        }
        return result;
    }

    public static Map<String, String> getInfo1(String line) throws Exception {
        Matcher matcher = pattern1.matcher(line);
        Map<String, String> result = null;
        if (matcher.matches()) {
            int groups = matcher.groupCount();

            if (groups == mapKeys.length) {
                result = new HashMap<>();
                for (int index = 0; index < groups; index++) {
                    result.put(mapKeys[index], matcher.group(index + 1));
                }
            } else {
                LOGGER.debug("Parse line " + line + " with regex error: inconsistent groups");
            }
        } else {
            LOGGER.debug("Parse line " + line + " with regex error: regular expression mismatch");
        }
        return result;
    }

}
