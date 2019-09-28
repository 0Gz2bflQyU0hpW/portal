package com.weibo.dip.data.platform.datacubic.youku.test;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;
import java.util.function.Consumer;

/**
 * Created by delia on 2017/1/13.
 */
public class SparkSreamingMain {
    private static String TOPIC_RECV = "xiaoyu";
    private static String TOPIC_RES = "xiaoyures";
//    private static String TOPIC_RES2 = "xiaoyuhehe";

    private static Map<String, Object> getProducerConf() {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "10.210.136.34:9092,10.210.136.80:9092,10.210.77.15:9092");
        config.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return config;
    }

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(jsc, Durations.seconds(2));

        String zklist = "10.210.136.34:2181";
        String consGrp = "xiaoyuGrp";
        Map<String, Integer> topics = new HashMap<>();
        topics.put(TOPIC_RECV, 1);

        int receivers = 1;
        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(receivers);
        JavaPairReceiverInputDStream<String, String> jpds = KafkaUtils.createStream(javaStreamingContext, zklist, consGrp, topics);
        for (int index = 0; index < receivers; index++) {
            kafkaStreams.add(jpds);
        }

        JavaPairDStream<String, String> unionStream = javaStreamingContext.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));

        JavaDStream<String> sourceStream = unionStream.map(tuple -> tuple._2);
        sourceStream.foreachRDD(stringJavaRDD->{
            stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                @Override
                public void call(Iterator<String> stringIterator) throws Exception {

                    Producer<String, String> producer = new KafkaProducer<String, String>(getProducerConf());
                    stringIterator.forEachRemaining(new Consumer<String>() {
                        @Override
                        public void accept(String s) {
                            if (StringUtils.isEmpty(s)) return;
                            String res = "received === " + s;
                            producer.send(new ProducerRecord<String, String>(TOPIC_RES, res));
//                              producer.send(new ProducerRecord<String, String>(TOPIC_RES2, s));
                        }
                    });
                }
            });
        });

//        sourceStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//            @Override
//            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
//                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
//                    @Override
//                    public void call(Iterator<String> stringIterator) throws Exception {
//
//                        Producer<String, String> producer = new KafkaProducer<String, String>(getProducerConf());
//                        stringIterator.forEachRemaining(new Consumer<String>() {
//                            @Override
//                            public void accept(String s) {
//                                if (StringUtils.isEmpty(s)) return;
//                                String res = "received === " + s;
//                                producer.send(new ProducerRecord<String, String>(TOPIC_RES, res));
////                              producer.send(new ProducerRecord<String, String>(TOPIC_RES2, s));
//                            }
//                        });
//                    }
//                });
//
//            }
//        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }
}
