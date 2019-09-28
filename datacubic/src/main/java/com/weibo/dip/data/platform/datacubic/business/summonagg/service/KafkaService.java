package com.weibo.dip.data.platform.datacubic.business.summonagg.service;

import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.LongAccumulator;

/**
 * Created by liyang28 on 2018/7/28.
 */
public interface KafkaService {

  void setConfig() throws IOException;

  void sendKafka(JavaRDD<String> lineRdd, LongAccumulator matchCount) throws IOException;

}
