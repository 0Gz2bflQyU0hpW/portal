package com.weibo.dip.data.platform.falcon.kafka.Service;

import com.weibo.dip.data.platform.falcon.kafka.KafkaTools.KafkaTools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * Created by Wen on 2016/12/22.
 *
 */
public class KafkaService {


    public List<String> getRecentNumMessage(String zookeeper, String topic, String group_id, int num) throws Exception {
        int count=0;
        List<String> message=new ArrayList<>();
        long now=System.currentTimeMillis();
        ConsumerConnector consumer= KafkaTools.GetKafkaConsumer(zookeeper,group_id);
        long esp=System.currentTimeMillis()-now;
        System.out.println("escape Time"+esp);
        Map<String, Integer> topicMap = new HashMap<>();
        // Define single thread for topic
        topicMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);
        loop:
        for (final KafkaStream<byte[], byte[]> stream : streamList) {
            for (MessageAndMetadata<byte[], byte[]> aStream : stream) {
                String mes = new String(aStream.message());
                System.out.println("Message from Single Topic :: " + mes);
                message.add(mes);
                count++;
                if (count == num)
                    break loop;
            }
        }
        consumer.shutdown();
        new KafkaTools().removeInvalidConsumer(zookeeper,group_id);
        return message;
    }

}
