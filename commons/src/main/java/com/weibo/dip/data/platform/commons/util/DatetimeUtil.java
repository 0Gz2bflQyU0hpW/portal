package com.weibo.dip.data.platform.commons.util;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;

/**
 * Datetime util.
 *
 * @author yurun
 */
public class DatetimeUtil {
  public static final FastDateFormat DATETIME_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss");

  public static final FastDateFormat COMMON_DATETIME_FORMAT =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

  public static final FastDateFormat ISO_8601_EXTENDED_DATETIME_FORMAT =
      DateFormatUtils.ISO_8601_EXTENDED_DATETIME_FORMAT;
}
