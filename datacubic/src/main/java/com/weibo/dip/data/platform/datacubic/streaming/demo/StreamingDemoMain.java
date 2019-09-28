package com.weibo.dip.data.platform.datacubic.streaming.demo;

import com.weibo.dip.data.platform.datacubic.streaming.core.DipStreamingContext;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yurun on 17/3/2.
 */
public class StreamingDemoMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingDemoMain.class);

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf();

        DipStreamingContext streamingContext = new DipStreamingContext(sparkConf, Durations.seconds(5));

        String topic = "demo_streaming_source";

        int receivers = 1;

        String zkQuorums = "d013004044.hadoop.dip.weibo.com:2181/kafka_intra";

        String consumerGroup = "demo_streaming";

        Map<String, Integer> topics = new HashMap<>();

        topics.put(topic, 1);

        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(receivers);

        for (int index = 0; index < receivers; index++) {
            kafkaStreams.add(KafkaUtils.createStream(streamingContext, zkQuorums, consumerGroup, topics));
        }

        JavaPairDStream<String, String> unionStream = streamingContext.union(kafkaStreams.get(0),
            kafkaStreams.subList(1, kafkaStreams.size()));

        JavaDStream<String> sourceStream = unionStream.map(Tuple2::_2).map(String::trim);

        sourceStream.foreachRDD(rdd -> rdd.foreachPartition(iter -> {
            while (iter.hasNext()) {
                LOGGER.info("line: {}", iter.next().length());
            }
        }));

        streamingContext.start();
    }

}
