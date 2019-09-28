package com.weibo.dip.databus.kafka;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeWriteFileMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeWriteFileMain.class);

    private static final String zooKeeper = "first.zookeeper.dip.weibo.com:2181,second.zookeeper.dip.weibo.com:2181,third.zookeeper.dip.weibo.com:2181/kafka/k1001";
    private static final String groupId = "dip";
    private static final String topic = "mweibo_client_medialive_qa_log";
    private static int threads = 2;
    private static String baseDir = "/data0/kafka-topic-data";

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();

        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
//        props.put("auto.offset.reset", "smallest");

        return new ConsumerConfig(props);
    }


    public static class ConsumerClient implements Runnable {
        private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerClient.class);

        private KafkaStream stream;
        private int threadNumber;
        private BufferedWriter writer;
        private String filePath;

        public ConsumerClient(KafkaStream stream, int threadNumber) {
            this.stream = stream;
            this.threadNumber = threadNumber;

            File fileDir = new File(baseDir);
            if(!fileDir.exists() && !fileDir.isDirectory()){
                fileDir.mkdir();
            }
        }

        public void run() {
            LOGGER.info("current thread name: " + Thread.currentThread().getName());

            filePath = baseDir + "/" + topic + "-" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) + "-" + threadNumber;
            LOGGER.info(Thread.currentThread().getName() + " create file:" + filePath);

            try {
                ConsumerIterator<byte[], byte[]> iter = stream.iterator();

                writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(filePath))));

                while (iter.hasNext()) {
                    String msg = new String(iter.next().message());

                    writer.append(msg + "\n");

                    //test output
 //                   System.out.println(msg);
                }

                LOGGER.info("shutting down Thread: " + threadNumber);
            }catch (Exception e){
                try {
                    writer.close();
                } catch (IOException e1) {
                    LOGGER.error("close write error: " + e1.toString());
                }
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
    }

}