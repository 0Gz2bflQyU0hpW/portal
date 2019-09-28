package com.weibo.dip.data.platform.datacubic.live;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/2/20.
 */
public class LiveConsumerMain {

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put("zookeeper.connect", "first.zookeeper.dip.weibo.com:2181,second.zookeeper.dip.weibo.com:2181,third.zookeeper.dip.weibo.com:2181/kafka/k1001");
        properties.put("group.id", "yurun_test_test");
        //properties.put("auto.offset.reset", "smallest");

        ConsumerConfig config = new ConsumerConfig(properties);

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<>();

        String topic = "live_weibo_new";

        topicCountMap.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        ConsumerIterator<byte[], byte[]> iterator = streams.get(0).iterator();

        Pattern pattern = Pattern.compile("^([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)$");

        while (iterator.hasNext()) {
            MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();

            String line = new String(messageAndMetadata.message());

            Matcher matcher = pattern.matcher(line.trim());

            if (matcher.matches()) {
                String type = matcher.group(4);
                if (type.equals("user_msg")) {
                    System.out.println(type);
                }

            } else {
                System.out.println("false");
                System.out.print(line);
            }
        }

        consumer.shutdown();
    }

}
