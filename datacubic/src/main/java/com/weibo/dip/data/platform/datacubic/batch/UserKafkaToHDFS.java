package com.weibo.dip.data.platform.datacubic.batch;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.CharEncoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by yurun on 17/4/18.
 */
public class UserKafkaToHDFS {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();

        properties.put("zookeeper.connect", "d013004044.hadoop.dip.weibo.com:2181/kafka_intra");
        properties.put("group.id", "yurun_test");
        //properties.put("auto.offset.reset", "smallest");

        ConsumerConfig config = new ConsumerConfig(properties);

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<>();

        String topic = "usertrace";

        topicCountMap.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        ConsumerIterator<byte[], byte[]> iterator = streams.get(0).iterator();

        BufferedWriter writer = null;

        long count = 0;

        while (iterator.hasNext()) {
            MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();

            String line = new String(messageAndMetadata.message()).trim();

            if (writer == null) {
                writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("/tmp/usertrace/" + System.currentTimeMillis())), CharEncoding.UTF_8));
            }

            writer.write(line);
            writer.newLine();

            if (++count >= 10000000) {
                writer.close();
                writer = null;

                count = 0;
            }
        }

        consumer.shutdown();
    }

}
