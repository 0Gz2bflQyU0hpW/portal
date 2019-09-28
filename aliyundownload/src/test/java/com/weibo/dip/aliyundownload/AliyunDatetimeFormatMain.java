package com.weibo.dip.aliyundownload;

import java.util.Date;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.lang3.time.FastTimeZone;

/** @author yurun */
public class AliyunDatetimeFormatMain {
  private static final FastDateFormat ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z =
      FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ssZZ", FastTimeZone.getGmtTimeZone());

  public static void main(String[] args) {
    System.out.println(ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z.format(new Date()));
  }
}
