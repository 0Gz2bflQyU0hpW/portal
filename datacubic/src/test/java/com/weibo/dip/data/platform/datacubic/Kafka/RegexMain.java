package com.weibo.dip.data.platform.datacubic.Kafka;

import com.weibo.dip.data.platform.datacubic.streaming.Constants;

import java.util.Properties;

/**
 * Created by yurun on 17/2/20.
 */
public class RegexMain {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();

        properties.load(RegexMain.class.getClassLoader().getResourceAsStream(Constants.SOURCE_CONFIG));

        String regex = properties.getProperty("source.kafka.topic.regex");

        System.out.println(regex);
    }

}
