package com.weibo.dip.data.platform.datacubic.videotrace.ha;

import org.apache.commons.lang.CharEncoding;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/1/11.
 */
public class HAMain {

    public static void main(String[] args) throws Exception {
        Pattern pattern = Pattern.compile("^([^\\s]*) ?([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*):([^\\s]*) \\[([^\\s]*)] ([^\\s]*) ([^\\s]*) ([^\\s]*)/([^\\s]*)/([^\\s]*)/([^\\s]*)/([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) \\{([^\\s]*)\\|([^\\s]*)\\|([^\\s]*)} \"(.*)$");

        BufferedReader reader = new BufferedReader(new InputStreamReader(HAMain.class.getClassLoader().getResourceAsStream("ha.log"), CharEncoding.UTF_8));

        String line;

        while ((line = reader.readLine()) != null) {
            Matcher matcher = pattern.matcher(line);

            if (matcher.matches()) {
                int count = matcher.groupCount();

                for (int index = 1; index <= count; index++) {
                    System.out.println(index + ": " + matcher.group(index));
                }
            }
        }

        reader.close();
    }

}
