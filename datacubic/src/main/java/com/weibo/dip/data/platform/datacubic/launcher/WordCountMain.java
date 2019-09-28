package com.weibo.dip.data.platform.datacubic.launcher;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by yurun on 17/3/24.
 */
public class WordCountMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountMain.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext(conf);

        List<String> words = Arrays.asList("a", "b", "c", "d", "e", "f", "g");

        long count = context.parallelize(words)
            .map(word -> {
                return word.toUpperCase();
            })
            .count();

        context.stop();

        System.out.println("count: " + count);
    }

}
