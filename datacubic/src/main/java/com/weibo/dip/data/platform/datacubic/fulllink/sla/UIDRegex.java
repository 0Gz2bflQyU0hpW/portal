package com.weibo.dip.data.platform.datacubic.fulllink.sla;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/7/11.
 */
public class UIDRegex {

    public static void main(String[] args) {
        String line = "010\",\"networktype\":\"wifi\",\"uid\":\"2698253212\",\"sessionid\":\"79ADDBDD-7B6C-413E-AB02-CD74ACF58CB7\"}";

        String regex = "^.*\"uid\":\"(\\d+)\".*$";

        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(line);

        if (matcher.matches()) {
            System.out.println(matcher.group(1));
        }
    }

}
