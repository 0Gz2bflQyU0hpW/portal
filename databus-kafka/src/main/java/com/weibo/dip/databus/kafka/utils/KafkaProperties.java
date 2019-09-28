package com.weibo.dip.databus.kafka.utils;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class KafkaProperties {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProperties.class);
    private static Properties properties;

    private KafkaProperties(){}

    static{
        properties = new Properties();
        try {
            properties.load(new BufferedReader(new InputStreamReader(KafkaProperties.class.getClassLoader().getResourceAsStream("kafka2hdfs.properties"))));
        } catch (IOException e) {
            LOGGER.error("load properties file error \n{}", ExceptionUtils.getFullStackTrace(e));
            throw new ExceptionInInitializerError(e);
        }
    }

    public static Properties getInstance(){
        return properties;
    }
}
