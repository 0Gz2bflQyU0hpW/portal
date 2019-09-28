package com.weibo.dip.data.platform.falcon.kafka.metric;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jianhong1 on 2018/7/26.
 */
public class KafkaState {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaState.class);
  private static final int BROKER_PORT = 9092;
  private static final int SO_TIMEOUT = 100000;

  //get latest offset of partition
  public static long getLatestOffset(SimpleConsumer simpleConsumer, String topic, int partition, String clientName){
    PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1);

    HashMap<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
    requestInfo.put(new TopicAndPartition(topic, partition), partitionOffsetRequestInfo);

    OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse response = simpleConsumer.getOffsetsBefore(request);

    if(response.hasError()){
      LOGGER.error("error fetching latest offset data, reason:{}", response.toString());
      return 0;
    }
    LOGGER.debug("response: {}", response.toString());

    long[] offsets = response.offsets(topic, partition);
    return offsets[0];
  }

  //get all partitions LogEndOffset of topic
  public static Map<String, Long> getLogEndOffsetsOfTopic(String topic, Map<String, String> leaderPartitions){
    Map<String, Long> logMap = new HashMap<>();

    for (Map.Entry<String, String> entry: leaderPartitions.entrySet()) {
      int partitionId = Integer.parseInt(entry.getKey());
      String leaderIp = entry.getValue();

      SimpleConsumer simpleConsumer = null;
      try{
        String clientName = String.format("%s_%s_%s", "Client", topic, partitionId);
        simpleConsumer = new SimpleConsumer(leaderIp, BROKER_PORT, SO_TIMEOUT, 64*1024, clientName);

        long logEndOffset = getLatestOffset(simpleConsumer, topic, partitionId, clientName);
        logMap.put(String.valueOf(partitionId), logEndOffset);
      }finally {
        if(simpleConsumer != null){
          simpleConsumer.close();
        }
      }
    }
    return logMap;
  }
}
