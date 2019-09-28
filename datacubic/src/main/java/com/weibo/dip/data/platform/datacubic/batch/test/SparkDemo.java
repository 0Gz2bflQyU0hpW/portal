package com.weibo.dip.data.platform.datacubic.batch.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 17/11/23.
 */
public class SparkDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkDemo.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext(conf);

        long count = context.textFile(args[0]).count();

        context.stop();

        LOGGER.info("count: " + count);
    }

}
