package com.weibo.dip.data.platform.falcon.transport.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Wen on 2017/2/10.
 */
public class KafkaSender implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSender.class);

    private static final String KAFKA_SERVERS_KEY = "bootstrap.servers";
    private static final String KAFKA_KEY_SERIALIZER_KEY = "key.serializer";
    private static final String KAFKA_VALUE_SERIALIZER_KEY = "value.serializer";
    private static final String KAFKA_COMPRESSION_CODEC_KEY = "compression.type";

    private static final String KAFKA_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String KAFKA_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String KAFKA_COMPRESSION_CODEC = "gzip";

    private static final KafkaSender KAFKA_SENDER = new KafkaSender();

    private Producer<String, String> producer;

    private String topic;

    public static KafkaSender getInstance() {
        return KAFKA_SENDER;
    }

    private KafkaSender() {
        Map<String, Object> config = new HashMap<>();

        config.put(KAFKA_SERVERS_KEY, ConfigUtils.KAFKA_SERVERS);

        config.put(KAFKA_KEY_SERIALIZER_KEY, KAFKA_KEY_SERIALIZER);

        config.put(KAFKA_VALUE_SERIALIZER_KEY, KAFKA_VALUE_SERIALIZER);

        config.put(KAFKA_COMPRESSION_CODEC_KEY, KAFKA_COMPRESSION_CODEC);

        producer = new KafkaProducer<>(config);

        topic = ConfigUtils.KAFKA_TOPIC_NAME;
    }

    public void sendMessgage(String message) throws IOException {
        producer.send(new ProducerRecord<>(topic, message));
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            producer.close();
        }
    }

}
