package com.weibo.dip.aliyundownload;

import java.text.SimpleDateFormat;

/** @author yurun */
public class LogNameTester {

  private static String buildHDFSPath(String logName) throws Exception {
    String hdfsBase = "/user/yurun/rawlog";

    String category = "www_spoollxrsaansnq8tjw0_aliyunXweibo";

    SimpleDateFormat aliyunDF = new SimpleDateFormat("yyyyMMddHHmm");
    SimpleDateFormat hdfsDF = new SimpleDateFormat("yyyy_MM_dd/HH");

    SimpleDateFormat aliDefaultDF = new SimpleDateFormat("yyyy_MM_dd_HHmmss");

    try {
      return hdfsBase
          + "/"
          + category
          + "/"
          + hdfsDF.format(aliyunDF.parse(logName.split("-", -1)[0]))
          + "/"
          + logName;
    } catch (Exception e) {
      return hdfsBase
          + "/"
          + category
          + "/"
          + hdfsDF.format(
              aliDefaultDF.parse(
                  logName.substring(logName.indexOf("_") + 1, logName.lastIndexOf("_"))))
          + "/"
          + logName;
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println(buildHDFSPath("zzx.sinaimg.cn_2018_04_26_000000_010000.gz"));
  }
}
