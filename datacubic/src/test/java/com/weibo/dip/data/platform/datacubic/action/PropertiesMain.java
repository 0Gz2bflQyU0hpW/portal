package com.weibo.dip.data.platform.datacubic.action;

import org.apache.commons.lang.CharEncoding;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by yurun on 17/1/18.
 */
public class PropertiesMain {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();

        properties.load(new BufferedReader(new InputStreamReader(PropertiesMain.class.getClassLoader().getResourceAsStream("source.confi"), CharEncoding.UTF_8)));

        String regex = properties.getProperty("source.kafka.topic.regex");

        System.out.println(regex);

        BufferedReader reader = new BufferedReader(new InputStreamReader(PropertiesMain.class.getClassLoader().getResourceAsStream("source.confi"), CharEncoding.UTF_8));

        String line = null;

        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }

}
