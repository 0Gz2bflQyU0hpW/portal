package com.weibo.dip.data.platform.datacubic.Kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/2/20.
 */
public class OpenAPIOPMain {

    private static String getRegex() throws Exception {
        Properties properties = new Properties();

        properties.load(OpenAPIOPMain.class.getClassLoader().getResourceAsStream("source.config"));

        String regex = properties.getProperty("source.kafka.topic.regex");

        //regex = "^\\[(\\S+)] ([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\\s$";

        return regex;
    }

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();

        properties.put("zookeeper.connect", "first.zookeeper.dip.weibo.com:2181,second.zookeeper.dip.weibo.com:2181,third.zookeeper.dip.weibo.com:2181/kafka/k1001");
        properties.put("group.id", "yurun_test");
        //properties.put("auto.offset.reset", "smallest");

        ConsumerConfig config = new ConsumerConfig(properties);

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<>();

        String topic = "openapi_op";

        topicCountMap.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        ConsumerIterator<byte[], byte[]> iterator = streams.get(0).iterator();

        long all = 100000;

        long count = 0;

        long match = 0;

        String regex = getRegex();

        System.out.println(regex);

        Pattern pattern = Pattern.compile(regex);

        List<String> notMatch = new ArrayList<>();

        while (iterator.hasNext()) {
            MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();

            String line = new String(messageAndMetadata.message());

            Matcher matcher = pattern.matcher(line);

            if (matcher.matches()) {
                match++;
            } else {
                notMatch.add(line);

                System.out.println("not match: " + notMatch.size() + "[" + match + "]");
            }

            if (++count >= all) {
                break;
            }
        }

        consumer.shutdown();

        for (String line : notMatch) {
            System.out.println(line);
        }

        System.out.println("count: " + count + ", match: " + match);
    }

}
