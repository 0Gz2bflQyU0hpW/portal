package com.weibo.dip.data.platform.datacubic.test;

import org.apache.commons.lang.CharEncoding;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/8/24.
 */
public class PlatformRegexMain {

    public static void main(String[] args) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(PlatformRegexMain.class
            .getClassLoader()
            .getResourceAsStream("platform.log"), CharEncoding.UTF_8));

        String line = reader.readLine();

        reader.close();

        String regex = "([^\\s]*) ([^\\s]*) ([^\\s]*) - \\[(.*)] \"(.*)\" ([^\\s]*) " +
            "([^\\s]*) \"(.*)\" \"(.*)\" \"(.*)\" \"(.*)\" \"(.*)\" \"(.*)\" \"(" +
            ".*)\" \"(.*)\"";

        String regex2 = "([^\\s]+) ([^\\s]+) ([^\\s]+) - \\[([^\"]+)] \"([^\"]+)\" ([^\\s]+) " +
            "([^\\s]+) \"([^\"]+)\" \"([^\"]+)\" \"([^\"]+)\" \"([^\"]+)\" \"([^\"]+)\" \"[^\"]+\" \"(" +
            "[^\"]+)\" \"([^\"]+)\"";

        System.out.println(regex);
        System.out.println(regex2);

        Pattern pattern = Pattern.compile(regex2);

        Matcher matcher = null;

        long begin = System.currentTimeMillis();

        for (int index = 0; index < 300000000; index++) {
            matcher = pattern.matcher(line);
        }

        long end = System.currentTimeMillis();

        System.out.println(end - begin);

        if (matcher.matches()) {
            for (int index = 1; index <= matcher.groupCount(); index++) {
                System.out.println(index + " : " + matcher.group(index));
            }
        }
    }

}
