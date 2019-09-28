package com.weibo.dip.data.platform.datacubic.Kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.*;

/**
 * Created by yurun on 17/2/20.
 */
public class KafkaConsumerMain {

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put("zookeeper.connect", "10.13.80.21:2181,10.13.80.22:2181,10.13.80.23:2181/kafka/k1001");
        properties.put("group.id", "yurun_test");
        //properties.put("auto.offset.reset", "smallest");

        ConsumerConfig config = new ConsumerConfig(properties);

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<>();

        String topic = "openapi_v4";

        topicCountMap.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        ConsumerIterator<byte[], byte[]> iterator = streams.get(0).iterator();

        while (iterator.hasNext()) {
            MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();

            String line = new String(messageAndMetadata.message());

            System.out.println(line.trim());
        }

        consumer.shutdown();
    }

}
