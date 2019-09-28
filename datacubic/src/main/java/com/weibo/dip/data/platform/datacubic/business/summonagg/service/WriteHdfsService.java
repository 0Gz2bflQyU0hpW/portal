package com.weibo.dip.data.platform.datacubic.business.summonagg.service;

import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by liyang28 on 2018/7/28.
 */
public interface WriteHdfsService {

  void sendHdfs(JavaRDD<String> lineRdd, String outputPath, long currentTimeMillis)
      throws IOException;
}
