package com.weibo.dip.data.platform.kafka;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by yurun on 17/7/16.
 */
public class KafkaAdmin {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdmin.class);

    private String[] servers;
    private int port;
    private int soTimeout;
    private int bufferSize;

    private String clientId = KafkaAdmin.class.getName();

    public KafkaAdmin(String[] servers, int port, int soTimeout, int bufferSize) {
        this.servers = servers;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
    }

    public List<String> getTopicNames() throws Exception {
        for (String seed : servers) {
            SimpleConsumer client = null;

            try {
                client = new SimpleConsumer(seed, port, soTimeout, bufferSize, clientId);

                TopicMetadataResponse topicMetadataResponse = client.send(
                    new TopicMetadataRequest(Collections.emptyList()));

                return topicMetadataResponse.topicsMetadata()
                    .stream()
                    .map(TopicMetadata::topic)
                    .collect(Collectors.toList());
            } catch (Exception e) {
                LOGGER.warn("get response from {} error: {}", seed, ExceptionUtils.getFullStackTrace(e));
            } finally {
                if (Objects.nonNull(client)) {
                    client.close();
                }
            }
        }

        throw new RuntimeException("kafka servers unavailable");
    }

    public List<Partition> getPartitions(String topic) throws Exception {
        for (String seed : servers) {
            SimpleConsumer client = null;

            try {
                client = new SimpleConsumer(seed, port, soTimeout, bufferSize, clientId);

                TopicMetadataResponse topicMetadataResponse = client.send(
                    new TopicMetadataRequest(Collections.singletonList(topic)));

                List<TopicMetadata> topicMetadatas = topicMetadataResponse.topicsMetadata();
                if (CollectionUtils.isEmpty(topicMetadatas)) {
                    return null;
                }

                List<PartitionMetadata> partitionMetadatas = topicMetadatas.get(0).partitionsMetadata();

                List<Partition> partitions = new ArrayList<>(partitionMetadatas.size());

                for (PartitionMetadata partitionMetadata : partitionMetadatas) {
                    int id = partitionMetadata.partitionId();
                    Broker leader = partitionMetadata.leader();
                    List<Broker> replicas = partitionMetadata.replicas();
                    List<Broker> isr = partitionMetadata.isr();

                    long offset = 0L;

                    if (Objects.nonNull(leader)) {
                        SimpleConsumer partitionClient = null;

                        try {
                            partitionClient = new SimpleConsumer(leader.host(), port, soTimeout, bufferSize,
                                clientId);

                            OffsetRequest partitionOffsetRequest = new OffsetRequest(
                                Collections.singletonMap(
                                    new TopicAndPartition(topic, id),
                                    new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1)),
                                kafka.api.OffsetRequest.CurrentVersion(),
                                clientId);

                            OffsetResponse partitionOffsetResponse = partitionClient.getOffsetsBefore(
                                partitionOffsetRequest);

                            offset = partitionOffsetResponse.offsets(topic, id)[0];
                        } catch (Exception e) {
                            LOGGER.warn("request partition offset error: {}",
                                ExceptionUtils.getFullStackTrace(e));
                        } finally {
                            if (Objects.nonNull(partitionClient)) {
                                partitionClient.close();
                            }
                        }
                    } else {
                        LOGGER.warn("topic {} partition[{}] 's leader is null", topic, id);
                    }

                    partitions.add(new Partition(topic, id, offset, leader, replicas, isr));
                }

                return partitions;
            } catch (Exception e) {
                LOGGER.warn("get response from {} error: {}", seed, ExceptionUtils.getFullStackTrace(e));
            } finally {
                if (Objects.nonNull(client)) {
                    client.close();
                }
            }
        }

        throw new RuntimeException("kafka servers unavailable");
    }

    public long getTopicOffset(String topic) throws Exception {
        List<Partition> partitions = getPartitions(topic);
        if (CollectionUtils.isEmpty(partitions)) {
            return 0L;
        }

        return partitions.stream()
            .map(Partition::getOffset)
            .reduce((a, b) -> a + b)
            .orElse(0L);
    }

    private class TopicPartitionOffsetFetcher implements Runnable {

        private String topic;

        private Map<String, Long> offsets;

        public TopicPartitionOffsetFetcher(String topic, Map<String, Long> offsets) {
            this.topic = topic;

            this.offsets = offsets;
        }

        @Override
        public void run() {
            long offset = 0L;

            try {
                offset = getTopicOffset(topic);
            } catch (Exception e) {
                LOGGER.warn("get topic[{}] offset error: " + ExceptionUtils.getFullStackTrace(e));
            }

            offsets.put(topic, offset);
        }

    }

    public Map<String, Long> getTopicOffsets(int threads) throws Exception {
        if (threads <= 0 || threads > Runtime.getRuntime().availableProcessors()) {
            threads = Runtime.getRuntime().availableProcessors() - 1;
        }

        List<String> topics = getTopicNames();
        if (CollectionUtils.isEmpty(topics)) {
            return null;
        }

        Map<String, Long> offsets = Collections.synchronizedMap(new HashMap<>());

        ExecutorService executor = Executors.newFixedThreadPool(threads);

        for (int index = 0; index < topics.size(); index++) {
            executor.submit(new TopicPartitionOffsetFetcher(topics.get(index), offsets));
        }

        executor.shutdown();

        while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            LOGGER.info("get topic offsets ...");
        }

        return offsets;
    }

    public void close() {
        // do nothing
    }

}
