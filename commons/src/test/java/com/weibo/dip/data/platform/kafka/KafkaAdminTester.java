package com.weibo.dip.data.platform.kafka;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by yurun on 17/7/16.
 */
public class KafkaAdminTester {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminTester.class);

    private static void getTopicNames(KafkaAdmin admin) throws Exception {
        List<String> topicNames = admin.getTopicNames();
        if (CollectionUtils.isNotEmpty(topicNames)) {
            topicNames.forEach(LOGGER::info);
        } else {
            LOGGER.warn("topic names is empty");
        }
    }

    private static void getPartitions(KafkaAdmin admin) throws Exception {
        String topic = "openapi_op";

        List<Partition> partitions = admin.getPartitions(topic);
        if (CollectionUtils.isNotEmpty(partitions)) {
            partitions.forEach(partition -> LOGGER.info(partition.toString()));
        } else {
            LOGGER.warn("Topic {} 's partitions is empty", topic);
        }
    }

    private static void getTopicOffset(KafkaAdmin admin) throws Exception {
        String topic = "openapi_op";

        long offset = admin.getTopicOffset(topic);

        LOGGER.info("topic {} offset: {}", topic, offset);
    }

    public static void main(String[] args) throws Exception {
        String[] servers = {"first.kafka.dip.weibo.com", "second.kafka.dip.weibo.com",
            "third.kafka.dip.weibo.com", "fourth.kafka.dip.weibo.com", "fifth.kafka.dip.weibo.com"};

        int port = 9092;

        int soTimeout = 10000;

        int bufferSize = 64 * 1024;

        long beginTime = System.currentTimeMillis();

        KafkaAdmin admin = new KafkaAdmin(servers, port, soTimeout, bufferSize);

        Map<String, Long> topicOffsets = admin.getTopicOffsets(5);
        if (MapUtils.isNotEmpty(topicOffsets)) {
            for (Map.Entry<String, Long> entry : topicOffsets.entrySet()) {
                LOGGER.info("topic: {}, offset: {}", entry.getKey(), entry.getValue());
            }
        }

        long endTime = System.currentTimeMillis();

        LOGGER.info("consume time: " + (endTime - beginTime));

        admin.close();
    }

}
