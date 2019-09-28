package com.weibo.dip.data.platform.datacubic.business.summonagg.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;

/**
 * Created by liyang28 on 2018/7/28.
 */
public interface ReadHdfsService {

  JavaRDD<Row> getHdfsDataRdd(JavaSparkContext sc, String inputPath,
      String[] fieldNames, LongAccumulator notMatchCount);
}
