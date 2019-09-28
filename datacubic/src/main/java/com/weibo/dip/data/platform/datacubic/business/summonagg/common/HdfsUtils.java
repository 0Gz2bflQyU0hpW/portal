package com.weibo.dip.data.platform.datacubic.business.summonagg.common;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liyang28 on 2018/7/28.
 */
public class HdfsUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsUtils.class);
  private static final FastDateFormat SDF1 = FastDateFormat.getInstance("yyyy_MM_dd");
  private static final FastDateFormat SDF2 = FastDateFormat.getInstance("yyyyMMdd");
  private static final String PREFIX_PATH = "/user/hdfs/summon/agglog/";
  public static final String SUFFIX_PATH = "/*";

  /**
   * 获得输入hdfs的路径 .
   */
  public static String getInputPath(String businessBefore) {
    return PREFIX_PATH + businessBefore + "/" + getYesterday();
  }

  /**
   * 获得hdfs输出路径 .
   */
  public static String getOutputPath(String businessAfter) {
    return PREFIX_PATH + businessAfter + "/" + getYesterday();
  }

  /**
   * 获得昨天的格式：yyyy_mm_dd .
   */
  public static String getYesterday() {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, -1);
    Date time = cal.getTime();
    return SDF1.format(time);
  }

  /**
   * 获得昨天的格式：yyyymmdd .
   */
  public static String getIndexYesterday() {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, -1);
    Date time = cal.getTime();
    return SDF2.format(time);
  }

  /**
   * 输入的hdfs路径是否存在且是否有文件 .
   */
  public static Boolean isExistsInputPathAndPathHaveFile(String inputPath) throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path inputHdfsPath = new Path(inputPath);
    if (fs.exists(inputHdfsPath)) {
      FileStatus[] fileList = fs.listStatus(inputHdfsPath);
      LOGGER.info("{} have file length {}", inputPath, fileList.length);
      return fileList.length > 0;
    }
    return false;
  }


}
