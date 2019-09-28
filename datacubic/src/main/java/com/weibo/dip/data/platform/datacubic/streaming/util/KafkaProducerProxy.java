package com.weibo.dip.data.platform.datacubic.streaming.util;

import com.weibo.dip.data.platform.datacubic.streaming.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Map<String, Object> config;

    public KafkaProducerProxy(Map<String, Object> config) {
        this.config = config;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (Objects.nonNull(producer)) {
                producer.close();

                LOGGER.info("kafka producer closed");
            }
        }));
    }

    private static Map<String, Object> getDefaultConfig(String[] servers) {
        Map<String, Object> config = new HashMap<>();

        config.put(BOOTSTRAP_SERVERS, String.join(Constants.COMMA, servers));
        config.put(KEY_SERIALIZER, STRING_SERIALIZER);
        config.put(VALUE_SERIALIZER, STRING_SERIALIZER);

        return config;
    }

    public KafkaProducerProxy(String[] servers) {
        this(getDefaultConfig(servers));
    }

    public void send(String topic, String key, String value) {
        if (Objects.isNull(producer)) {
            synchronized (LOCK) {
                if (Objects.isNull(producer)) {
                    producer = new KafkaProducer<>(config);

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
