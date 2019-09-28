package com.weibo.dip.data.platform.falcon.scheduler.util;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Wen on 2017/2/16.
 */
public class ConfigUtils {
    public static final long TIME_HDFS_INTERVAL ;
//    public static final long KAFKA_DELAY_TIME;
//    public static final long PRODUCER_GET_INTERVAL;
//    public static final int GET_THREAD_NUM;
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class);
    static{
        ClassLoader classLoader = ConfigUtils.class.getClassLoader();
        Properties prop = new Properties();
        InputStream is =classLoader.getResourceAsStream("scheduler.properties");
        try {
            prop.load(is);
        } catch (IOException e) {
            LOGGER.error(ExceptionUtils.getFullStackTrace(e));
        }
        TIME_HDFS_INTERVAL = Long.parseLong((String)prop.get("time.hdfs.interval"));
//        KAFKA_DELAY_TIME = Long.parseLong((String)prop.get("kafka.delay.time"));
//        PRODUCER_GET_INTERVAL = Long.parseLong((String)prop.get("get.strategy"));
//        GET_THREAD_NUM = Integer.parseInt((String)prop.get("num.get.threads"));
    }
    public static int getThreadNum(String jobType) {
        ClassLoader classLoader = ConfigUtils.class.getClassLoader();
        Properties prop = new Properties();
        InputStream is =classLoader.getResourceAsStream("scheduler.properties");
        try {
            prop.load(is);
        } catch (IOException e) {
            LOGGER.error(ExceptionUtils.getFullStackTrace(e));
        }
        System.out.println(jobType); //
        return Integer.parseInt((String)prop.get("nums." + jobType + ".threads"));
    }

//    public static void main(String[] args) {
//        ClassLoader classLoader = ConfigUtils.class.getClassLoader();
//        Properties prop = new Properties();
//        InputStream is =classLoader.getResourceAsStream("scheduler.properties");
//        try {
//            prop.load(is);
//        } catch (IOException e) {
//            LOGGER.error(ExceptionUtils.getFullStackTrace(e));
//        }
//        System.out.println(Integer.parseInt((String)prop.get("nums.1.threads")));
//    }
}
