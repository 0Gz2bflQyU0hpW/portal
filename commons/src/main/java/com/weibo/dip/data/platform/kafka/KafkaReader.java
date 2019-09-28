package com.weibo.dip.data.platform.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by yurun on 17/7/11.
 */
public class KafkaReader implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReader.class);

    private class StreamConsumer implements Runnable {

        private KafkaStream<byte[], byte[]> stream;

        public StreamConsumer(KafkaStream<byte[], byte[]> stream) {
            this.stream = stream;
        }

        @Override
        public void run() {
            for (MessageAndMetadata<byte[], byte[]> data : stream) {
                try {
                    buffer.put(new String(data.message(), CharEncoding.UTF_8));
                } catch (Exception e) {
                    LOGGER.warn("put data to buffer error: " + ExceptionUtils.getFullStackTrace(e));
                }
            }
        }

    }

    private String zookeeperPath;
    private String topic;
    private String consumerGroup;
    private int threads;
    private int bufferSize;

    private int bufferPollTimeout = 1;

    private ConsumerConnector consumer;

    private ExecutorService streamers = Executors.newCachedThreadPool();

    private BlockingQueue<String> buffer;

    public KafkaReader(String zookeeperPath, String topic, String consumerGroup,
                       int threads, int bufferSize) {
        this.zookeeperPath = zookeeperPath;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.threads = threads;
        this.bufferSize = bufferSize;

        List<KafkaStream<byte[], byte[]>> streams = getKafkaStreams(this.zookeeperPath, this.topic,
            this.consumerGroup, this.threads);

        if (bufferSize <= 0) {
            this.bufferSize = Integer.MAX_VALUE;
        }

        buffer = new LinkedBlockingDeque<>(this.bufferSize);

        for (KafkaStream<byte[], byte[]> stream : streams) {
            streamers.submit(new StreamConsumer(stream));
        }
    }

    private List<KafkaStream<byte[], byte[]>> getKafkaStreams(String zookeeperPath, String topic, String
        consumerGroup, int threads) {
        Properties properties = new Properties();

        properties.put("zookeeper.connect", zookeeperPath);
        properties.put("group.id", consumerGroup);

        ConsumerConfig config = new ConsumerConfig(properties);

        consumer = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<>();

        topicCountMap.put(topic, threads);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
            consumer.createMessageStreams(topicCountMap);

        return consumerMap.get(topic);
    }

    public String read() {
        String value;

        while (true) {
            try {
                value = buffer.poll(bufferPollTimeout, TimeUnit.SECONDS);
                if (Objects.nonNull(value)) {
                    return value;
                }
            } catch (InterruptedException e) {
                LOGGER.info("poll from buffer, but interrupted: " + ExceptionUtils.getFullStackTrace(e));
            }

            if (streamers.isTerminated() && buffer.isEmpty()) {
                return null;
            }
        }
    }

    @Override
    public void close() throws IOException {
        consumer.shutdown();

        streamers.shutdown();
    }

    public static void main(String[] args) {
        String zookeeperPath = "d013004044.hadoop.dip.weibo.com:2181/kafka_intra";
        String topic = "godeyes_collect";
        String consumerGroup = "godeyes";
        int threads = 1;
        int bufferSize = 1000;

        KafkaReader reader = new KafkaReader(zookeeperPath, topic, consumerGroup, threads, bufferSize);

        new Thread(() -> {
            try {
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {
                LOGGER.warn("thread sleep interrupted!");
            }

            try {
                reader.close();
            } catch (IOException e) {
                LOGGER.error("kafka reader close error: {}", ExceptionUtils.getFullStackTrace(e));
            }
        }).start();

        String value;

        while ((value = reader.read()) != null) {
            System.out.println(value);
        }
    }

}
