package com.weibo.dip.data.platform.datacubic.streaming.demo;

import com.weibo.dip.data.platform.datacubic.streaming.monitor.StreamingMonitor;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by yurun on 17/3/2.
 */
public class DirectStreamingMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectStreamingMain.class);

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local[3]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(5));

        String brokers = "d013004044.hadoop.dip.weibo.com:9092";

        String topic = "demo_streaming_source";

        Map<String, String> kafkaParams = new HashMap<>();

        kafkaParams.put("bootstrap.servers", brokers);

        Set<String> topics = new HashSet<>();

        topics.add(topic);

        JavaDStream<String> sourceStream = KafkaUtils.createDirectStream(streamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics).map(Tuple2::_2).map(String::trim);

        sourceStream.foreachRDD(rdd -> {
            LOGGER.info("count: " + rdd.count());
        });

        streamingContext.addStreamingListener(new StreamingMonitor(sparkConf.get("spark.app.name")));

        streamingContext.start();

        streamingContext.awaitTermination();
    }

}
