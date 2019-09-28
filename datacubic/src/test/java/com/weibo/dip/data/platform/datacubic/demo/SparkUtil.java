package com.weibo.dip.data.platform.datacubic.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Created by delia on 2017/2/23.
 */
public class SparkUtil {
    public static SparkConf getSparkConf(){
        return new SparkConf();
    }

    public static SparkConf getSparkConf(String master,String appName){
        return new SparkConf().setMaster(master).setAppName(appName);
    }

    public static SparkConf getLocalSparkConf(){
        return new SparkConf().setMaster("local").setAppName("test");
    }

    public static JavaSparkContext getJavaSparkContext(){
        return new JavaSparkContext(getSparkConf());
    }

    public static SparkSession getSparkSession(){
        SparkSession sparkSession = SparkSession.builder().sparkContext(getJavaSparkContext().sc()).enableHiveSupport().getOrCreate();
        return sparkSession;
    }
    public static SparkSession getSparkSession(JavaSparkContext context){
        SparkSession sparkSession = SparkSession.builder().sparkContext(context.sc()).enableHiveSupport().getOrCreate();
        return sparkSession;
    }
}
