package com.weibo.dip.data.platform.datacubic.business.summonagg.service.impl;

import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import com.weibo.dip.data.platform.datacubic.business.summonagg.service.WriteHdfsService;
import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;
import scala.Serializable;

/**
 * Created by liyang28 on 2018/7/28.
 */
public class WriteHdfsServiceImpl implements WriteHdfsService, Serializable {

  /**
   * write hdfs .
   */
  public WriteHdfsServiceImpl() {
  }

  @Override
  public void sendHdfs(JavaRDD<String> lineRdd, String outputPath, long currentTimeMillis)
      throws IOException {
    //落到hdfs,特殊情况，创建时间戳的
    if (HDFSUtil.exist(outputPath)) {
      outputPath = outputPath + "_" + currentTimeMillis;
    }
    lineRdd.repartition(24).saveAsTextFile(outputPath);
  }


}
