package com.weibo.dip.data.platform.datacubic.batch.aliyunoss;

import com.weibo.dip.data.platform.datacubic.batch.util.BatchUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by xiaoyu on 2017/6/14.
 */
public class AliyunOSSCount {
    private static Logger LOGGER = LoggerFactory.getLogger(AliyunOSSCount.class);

    private static final String ROOT_PATH = "/user/hdfs/rawlog/app_picserversweibof6vwt_multiupload";

    public static void main(String[] args) throws Exception{
        String inputPath = BatchUtils.getInputPathToday(ROOT_PATH,args);
        LOGGER.info("path: " + inputPath);
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf());
        JavaRDD<String> rdd = sparkContext.textFile(inputPath);
        LOGGER.info("rdd count: " + rdd.count() + " path: " + inputPath);
        sparkContext.close();

    }




}
