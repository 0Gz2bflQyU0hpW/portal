package com.weibo.dip.data.platform.datacubic.aliyun;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * AliYun log ungzip.
 *
 * @author yurun
 */
public class AliYunLogUnGzip {

  /**
   * Main.
   *
   * @param args hour
   */
  public static void main(String[] args) {
    String hdfsInputBase = "/user/hdfs/rawlog/www_spoollxrsaansnq8tjw0_aliyunXweibo/2018_04_26/";
    String hdfsOutputBase = "/tmp/www_spoollxrsaansnq8tjw0_aliyunXweibo/";

    String hdfsInputPath = hdfsInputBase + args[0];
    String hdfsOutputPath = hdfsOutputBase + args[0];

    SparkConf conf = new SparkConf();

    conf.setAppName("AliYunUnGzip_" + args[0]);

    JavaSparkContext context = new JavaSparkContext(conf);

    context.textFile(hdfsInputPath).saveAsTextFile(hdfsOutputPath);

    context.close();
  }
}
