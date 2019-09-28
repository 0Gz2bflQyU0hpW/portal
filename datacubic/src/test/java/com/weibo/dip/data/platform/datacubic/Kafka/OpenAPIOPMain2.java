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
public class OpenAPIOPMain2 {

    public static void main(String[] args) {
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

        SortedSet<String> keys = new TreeSet<>();

        Pattern pattern = Pattern.compile("^\\[(\\S+)] ([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\n$");

        Pattern pattern2 = Pattern.compile(",?([^,|=|>]+)=>");

        while (iterator.hasNext()) {
            MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();

            String line = new String(messageAndMetadata.message());

            Matcher matcher = pattern.matcher(line);

            if (matcher.matches()) {
                String code = matcher.group(5);

                if (code.equals("14000008") || code.equals("14000009")) {
                    String ten = matcher.group(11);

                    Matcher matcher2 = pattern2.matcher(ten);

                    while (matcher2.find()) {
                        keys.add(matcher2.group(1));
                    }

                    if (++count >= all) {
                        break;
                    }
                }
            }
        }

        consumer.shutdown();

        System.out.println(keys.toString());
    }

}
