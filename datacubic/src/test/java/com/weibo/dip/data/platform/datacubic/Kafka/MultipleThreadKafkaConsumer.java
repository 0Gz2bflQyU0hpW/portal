package com.weibo.dip.data.platform.datacubic.Kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by yurun on 17/7/6.
 */
public class MultipleThreadKafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerMain.class);

    private static final AtomicLong COUNT = new AtomicLong(0);

    private static class KafkaConsumer implements Runnable {

        private KafkaStream<byte[], byte[]> stream;

        public KafkaConsumer(KafkaStream<byte[], byte[]> stream) {
            this.stream = stream;
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

            while (iterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();

                if (messageAndMetadata != null) {
                    COUNT.incrementAndGet();
                }
            }
        }

    }

    public static void main(String[] args) {
        String zkConnect = "10.13.80.21:2181,10.13.80.22:2181,10.13.80.23:2181/kafka/k1001";

        String topic = args[0];

        String groupId = "Kafka_Consumer_Yurun_Test";

        int threads = 10;

        Properties properties = new Properties();

        properties.put("zookeeper.connect", zkConnect);
        properties.put("group.id", groupId);

        ConsumerConfig config = new ConsumerConfig(properties);

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<>();

        topicCountMap.put(topic, threads);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams
            (topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        ExecutorService executor = Executors.newCachedThreadPool();

        for (KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new KafkaConsumer(stream));
        }

        executor.shutdown();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.shutdown()));

        while (!executor.isTerminated()) {
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);

                System.out.println("kafka consume: " + COUNT.getAndSet(0));
            } catch (InterruptedException e) {
            }
        }
    }
}
