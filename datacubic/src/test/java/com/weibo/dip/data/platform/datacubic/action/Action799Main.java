package com.weibo.dip.data.platform.datacubic.action;

import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/1/13.
 */
public class Action799Main {

    public static void main(String[] args) throws Exception {
        String regex = "(\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3})\\|(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)";

        System.out.println(regex);

        Pattern pattern = Pattern.compile(regex);

        System.out.println(pattern.pattern());

        //BufferedReader reader = new BufferedReader(new InputStreamReader(Action799Main.class.getClassLoader().getResourceAsStream("799.log"), CharEncoding.UTF_8));

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/yurun/Downloads/app_weibomobilekafka1234_weibomobileaction799-172.16.140.83-29549-2016_12_28_09-2016122809117_1747926"), CharEncoding.UTF_8));

        int match = 0;
        int nomatch = 0;

        String line;

        while ((line = reader.readLine()) != null) {
            line = line.trim();

            if (StringUtils.isEmpty(line)) {
                continue;
            }

            Matcher matcher = pattern.matcher(line.trim());

            if (matcher.matches()) {
                match++;
            } else {
                nomatch++;
            }
        }

        reader.close();

        System.out.println(match);
        System.out.println(nomatch);
    }

}
