package com.weibo.dip.data.platform.kafka;

import org.apache.commons.lang.StringUtils;
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
 * Created by yurun on 17/7/11.
 */
public class KafkaWriter implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaWriter.class);

    private String servers;
    private String defaultTopic;

    private Producer<String, String> producer;

    public KafkaWriter(String servers) {
        this(servers, null);
    }

    public KafkaWriter(String servers, String defaultTopic) {
        this.servers = servers;
        this.defaultTopic = defaultTopic;

        producer = getProducer(this.servers);
    }

    private Producer<String, String> getProducer(String servers) {
        Map<String, Object> config = new HashMap<>();

        config.put("bootstrap.servers", servers);

        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(config);
    }

    public void write(String data) {
        if (StringUtils.isEmpty(defaultTopic)) {
            LOGGER.warn("default topic name isn't specified, skip");

            return;
        }

        producer.send(new ProducerRecord<>(defaultTopic, data));
    }

    public void write(String topic, String data) {
        producer.send(new ProducerRecord<>(topic, data));
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    public static void main(String[] args) throws Exception {
        String servers = "10.13.4.44:9092";
        String topic = "godeyes_collect";

        KafkaWriter writer = new KafkaWriter(servers, topic);

        writer.write("line1");
        writer.write("line2");
        writer.write("line3");

        writer.close();
    }

}
