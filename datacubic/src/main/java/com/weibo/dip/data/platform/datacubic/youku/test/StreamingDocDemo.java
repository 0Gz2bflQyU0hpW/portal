package com.weibo.dip.data.platform.datacubic.youku.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by delia on 2017/2/22.
 */
public class StreamingDocDemo {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override public Iterator<String> call(String x) {
                        return Arrays.asList(x.split(" ")).iterator();
                    }
                }
        );

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                });
//        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
//                new Function2<Integer, Integer, Integer>() {
//                    @Override public Integer call(Integer i1, Integer i2) {
//                        return i1 + i2;
//                    }
//                });

// Print the first ten elements of each RDD generated in this DStream to the console
//        wordCounts.print();

        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate

    }
}
