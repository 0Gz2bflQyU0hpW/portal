package com.weibo.dip.data.platform.falcon.transport.utils;

import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by Wen on 2017/2/13.
 */
public class ConfigUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class);

    private static final String HDFS_TO_KAFKA_PROPERTIES = "hdfs_to_kafka.properties";

    private static final String PRODUCER_CATEGORY_NAME_KEY = "producer.category.name";
    private static final String PRODUCER_GET_INTERVAL_KEY = "producer.get.interval";

    private static final String CONSUMER_THREAD_NUM_KEY = "consumer.thread.num";

    private static final String KAFKA_SERVERS_KEY = "kafka.servers";
    private static final String KAFKA_TOPIC_NAME_KEY = "kafka.topic.name";

    private static final String STOP_FILE_PATH_KEY = "stop.file.path";

    public static final String TIME_PATTERN = "yyyyMMddHHmmss";

    private static final String PRODUCER_ADDITION_START_KEY = "producer.addition.start";

    private static final String PRODUCER_ADDITION_END_KEY = "producer.addition.end";

    private static final String PRODUCER_ADDITION_INTERVAL_KEY = "producer.addition.interval";

    public static final long PRODUCER_ADDITION_START;
    public static final long PRODUCER_ADDITION_END;
    public static final long PRODUCER_ADDITION_INTERVAL;

    public static final String PRODUCER_CATEGORY_NAME;
    public static final long PRODUCER_GET_INTERVAL;

    public static final int CONSUMER_THREAD_NUM;

    public static final String KAFKA_SERVERS;
    public static final String KAFKA_TOPIC_NAME;

    public static final String STOP_FILE_PATH;
    private ConfigUtils() {
    }

    static {
        Properties prop = new Properties();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(ConfigUtils.class.getClassLoader().getResourceAsStream(HDFS_TO_KAFKA_PROPERTIES), CharEncoding.UTF_8));

            prop.load(reader);

            PRODUCER_CATEGORY_NAME = prop.getProperty(PRODUCER_CATEGORY_NAME_KEY);
            PRODUCER_GET_INTERVAL = Long.parseLong(prop.getProperty(PRODUCER_GET_INTERVAL_KEY));
            PRODUCER_ADDITION_START = DateUtils.String2long(prop.getProperty(PRODUCER_ADDITION_START_KEY));
            PRODUCER_ADDITION_END = DateUtils.String2long(prop.getProperty(PRODUCER_ADDITION_END_KEY));
            PRODUCER_ADDITION_INTERVAL = Long.parseLong(prop.getProperty(PRODUCER_ADDITION_INTERVAL_KEY));

            CONSUMER_THREAD_NUM = Integer.parseInt(prop.getProperty(CONSUMER_THREAD_NUM_KEY));

            KAFKA_SERVERS = prop.getProperty(KAFKA_SERVERS_KEY);
            KAFKA_TOPIC_NAME = prop.getProperty(KAFKA_TOPIC_NAME_KEY);

            STOP_FILE_PATH= prop.getProperty(STOP_FILE_PATH_KEY);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.error(HDFS_TO_KAFKA_PROPERTIES + " reader close error: " + ExceptionUtils.getFullStackTrace(e));
                }
            }
        }

    }
}