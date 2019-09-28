package com.weibo.dip.data.platform.datacubic.streaming.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yurun on 17/1/13.
 */
public class KafkaProducerFactory {

    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    private static final String KEY_SERIALIZER = "key.serializer";

    private static final String VALUE_SERIALIZER = "value.serializer";

    private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    private static final Map<String, Producer<String, String>> PRODUCERS = new HashMap<>();

    private KafkaProducerFactory() {

    }

    public static synchronized Producer<String, String> getProducer(String bootstrapServers) {
        if (!PRODUCERS.containsKey(bootstrapServers)) {
            Map<String, Object> config = new HashMap<>();

            config.put(BOOTSTRAP_SERVERS, bootstrapServers);

            config.put(KEY_SERIALIZER, STRING_SERIALIZER);

            config.put(VALUE_SERIALIZER, STRING_SERIALIZER);

            Producer<String, String> producer = new KafkaProducer<>(config);

            PRODUCERS.put(bootstrapServers, producer);
        }

        return PRODUCERS.get(bootstrapServers);
    }

    public static synchronized void close(String bootstrapServers) {
        if (PRODUCERS.containsKey(bootstrapServers)) {
            Producer<String, String> producer = PRODUCERS.remove(bootstrapServers);
            producer.close();
        }
    }

    public static synchronized void close() {
        for (Producer<String, String> producer : PRODUCERS.values()) {
            producer.close();
        }

        PRODUCERS.clear();
    }

}
