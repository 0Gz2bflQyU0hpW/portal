package com.weibo.dip.databus.source;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Source;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by yurun on 17/8/9.
 */
public class SingleTopicKafkaSource extends Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleTopicKafkaSource.class);

    private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    private static final String GROUP_ID = "group.id";
    private static final String TOPIC = "topic";
    private static final String THREADS = "threads";

    private static final String COMMA = ",";

    private static final int TOPIC_THREADS = 1;

    private static final long STOP_SLEEP = 3000L;

    private String zookeeperAddress;
    private String groupId;
    private String topic;
    private int threads;

    private ConsumerConnector kafkaConnector;

    private ExecutorService streamers = Executors.newCachedThreadPool();

    @Override
    public void setConf(Configuration conf) {
        name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN +
            SingleTopicKafkaSource.class.getSimpleName();

        zookeeperAddress = conf.get(ZOOKEEPER_CONNECT);
        Preconditions.checkState(StringUtils.isNotEmpty(zookeeperAddress),
            name + " " + ZOOKEEPER_CONNECT + " must be specified");

        groupId = conf.get(GROUP_ID);
        Preconditions.checkState(StringUtils.isNotEmpty(groupId),
            name + " " + GROUP_ID + " must be specified");

        topic = conf.get(TOPIC);
        Preconditions.checkState(StringUtils.isNotEmpty(topic),
            name + " " + TOPIC + " must be specified");

        threads = Integer.valueOf(conf.get(THREADS));
    }

    private class Streamer implements Runnable {

        private KafkaStream<byte[], byte[]> stream;

        public Streamer(KafkaStream<byte[], byte[]> stream) {
            this.stream = stream;
        }

        @Override
        public void run() {
            LOGGER.info(name + " streamer " + Thread.currentThread().getName() + " started");

            for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
                String topic = messageAndMetadata.topic();
                String data;
                try {
                    data = new String(messageAndMetadata.message(), CharEncoding.UTF_8);
                } catch (UnsupportedEncodingException e) {
                    LOGGER.warn("unsupport encode " + CharEncoding.UTF_8);

                    continue;
                }

                deliver(new Message(topic, data));
            }

            LOGGER.info(name + " streamer " + Thread.currentThread().getName() + " stoped");
        }

    }

    @Override
    public void start() {
        LOGGER.info("{} starting...", name);

        Properties properties = new Properties();

        properties.put(ZOOKEEPER_CONNECT, zookeeperAddress);
        properties.put(GROUP_ID, groupId);

        ConsumerConfig config = new ConsumerConfig(properties);

        kafkaConnector = Consumer.createJavaConsumerConnector(config);

        Map<String, List<KafkaStream<byte[], byte[]>>> topicStreams =
            kafkaConnector.createMessageStreams(Collections.singletonMap(topic, threads));

        topicStreams.get(topic).forEach(stream -> new Streamer(stream));

        LOGGER.info("{} started", name);
    }

    @Override
    public void stop() {
        LOGGER.info("{} stoping...", name);

        kafkaConnector.shutdown();

        streamers.shutdown();

        try {
            while (!streamers.awaitTermination(STOP_SLEEP, TimeUnit.MILLISECONDS)) {
                LOGGER.info("{} streamers await termination ...", name);
            }
        } catch (InterruptedException e) {
            LOGGER.warn("{} streamers await termination, but interrupted", name);
        }

        LOGGER.info(name + " stoped");
    }

}
