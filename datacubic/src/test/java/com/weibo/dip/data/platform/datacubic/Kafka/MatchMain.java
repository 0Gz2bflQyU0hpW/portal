package com.weibo.dip.data.platform.datacubic.Kafka;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/4/11.
 */
public class MatchMain {

    public static void main(String[] args) {
        String line = "[77.28.106] 2017-04-11 12:02:51\t58.23.250.144\t1769130901\t14000003\t25\t\t1\t4095318041692966\t2803301701\tatusers";

        Pattern pattern = Pattern.compile("\\[(\\d+\\.\\d+\\.\\d+)] ([^\t]+)(.*)");

        Matcher matcher = pattern.matcher(line);

        if (matcher.matches()) {
            for (int index = 1; index <= matcher.groupCount(); index++) {
                System.out.println(matcher.group(index));
            }
        }

        Pattern pattern2 = Pattern.compile("^\\[(\\S+)] ([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\n$");

        System.out.println(pattern2.pattern());
    }

}
