package com.weibo.dip.data.platform.datacubic.streaming.core;

import com.weibo.dip.data.platform.datacubic.streaming.Constants;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by yurun on 17/11/30.
 */
public class KafkaProducerProxy implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerProxy.class);

    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    private static final Object LOCK = new Object();

    private static Producer<String, String> producer;

    private class ProducerManager extends Thread {

        @Override
        public void run() {
            Jedis jedis = null;

            try {
                jedis = new Jedis(DipStreaming.REDIS_HOST, DipStreaming.REDIS_PORT);

                while (!jedis.hget(DipStreaming.SPARK_STREAMING, appName).equals(DipStreaming.STOPED)) {
                    try {
                        Thread.sleep(DipStreaming.WAITER_SLEEP);

                        LOGGER.debug("kafka producer running...");
                    } catch (InterruptedException e) {
                        LOGGER.warn("producer manager sleeped, but interrupt");

                        break;
                    }
                }

                if (Objects.nonNull(producer)) {
                    producer.close();
                    LOGGER.info("kafka producer closed");
                }
            } catch (Exception e) {
                LOGGER.error("producer manager run error: {}", ExceptionUtils.getFullStackTrace(e));
            } finally {
                if (Objects.nonNull(jedis)) {
                    jedis.close();
                }
            }
        }

    }

    private String appName;
    private Map<String, Object> config;

    public KafkaProducerProxy(String appName, Map<String, Object> config) {
        this.appName = appName;
        this.config = config;
    }

    private static Map<String, Object> getDefaultConfig(String[] servers) {
        Map<String, Object> config = new HashMap<>();

        config.put(BOOTSTRAP_SERVERS, String.join(Constants.COMMA, servers));
        config.put(KEY_SERIALIZER, STRING_SERIALIZER);
        config.put(VALUE_SERIALIZER, STRING_SERIALIZER);

        return config;
    }

    public KafkaProducerProxy(String appName, String[] servers) {
        this(appName, getDefaultConfig(servers));
    }

    public void send(String topic, String key, String value) {
        if (Objects.isNull(producer)) {
            synchronized (LOCK) {
                if (Objects.isNull(producer)) {
                    producer = new KafkaProducer<>(config);

                    new ProducerManager().start();

                    LOGGER.info("kafka producer created");
                }
            }
        }

        producer.send(new ProducerRecord<>(topic, key, value));
    }

    public void send(String topic, String value) {
        send(topic, null, value);
    }

}
