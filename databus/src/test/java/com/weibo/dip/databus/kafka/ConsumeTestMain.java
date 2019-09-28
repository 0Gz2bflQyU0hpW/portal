package com.weibo.dip.databus.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumeTestMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeTestMain.class);

    private static final String zooKeeper = "10.13.56.76:2181,10.13.56.73:2181,10.13.56.83:2181/kafka/k1002";
    private static final String groupId = "dip1";
    private static final String topic = "test1";
    private static int threads = 2;

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();

        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("auto.offset.reset", "smallest");

        return new ConsumerConfig(props);
    }

    public static class ConsumerClient implements Runnable {
        private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerClient.class);

        private KafkaStream stream;
        private int threadNumber;

        public ConsumerClient(KafkaStream stream, int threadNumber) {
            this.stream = stream;
            this.threadNumber = threadNumber;
        }

        public void run() {
            LOGGER.info("current thread name: " + Thread.currentThread().getName());

            try {
                ConsumerIterator<byte[], byte[]> iter = stream.iterator();

                while (iter.hasNext()) {
                    String msg = new String(iter.next().message());

                    //test output
//                    System.out.println(msg);
                }

                LOGGER.info("shutting down Thread: " + threadNumber);
            }catch (Exception e){
                LOGGER.error("consumer client exception: " + e.toString());
            }
        }

    }

    public static void main(String[] args) {
        if(args.length > 0){
            threads = Integer.valueOf(args[0]);
        }
        LOGGER.info("Thread number: " + threads);

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zooKeeper, groupId));
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, threads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        ExecutorService executor = Executors.newFixedThreadPool(threads);

        int threadNumber = 0;
        for (KafkaStream stream : streams) {
            executor.submit(new ConsumerClient(stream, threadNumber));
            threadNumber++;
        }

        executor.shutdown();
    }

}