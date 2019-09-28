package com.weibo.dip.data.platform.commons.util;

import java.util.Date;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author yurun */
public class RawlogUtilTester {
  private static final Logger LOGGER = LoggerFactory.getLogger(RawlogUtilTester.class);

  public static void main(String[] args) throws Exception {
    FastDateFormat format = DatetimeUtil.COMMON_DATETIME_FORMAT;

    String category = "app_picserversweibof6vwt_wapupload";

    Date beginTime = format.parse("2018-09-25 14:00:00");
    Date endTime = format.parse("2018-09-25 15:00:00");

    List<String> files = RawlogUtil.getRangeTimeFiles(category, beginTime, endTime);

    if (CollectionUtils.isNotEmpty(files)) {
      for (String file : files) {
        LOGGER.info(file);
      }
    }
  }
}
