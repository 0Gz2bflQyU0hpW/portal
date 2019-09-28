package com.weibo.dip.costing.util;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by yurun on 18/4/25.
 */
public class DatetimeUtil {

  public static Date monthBegin(String year, String month) {
    Calendar calendar = Calendar.getInstance();

    calendar.set(Calendar.YEAR, Integer.valueOf(year));
    calendar.set(Calendar.MONTH, Integer.valueOf(month) - 1);
    calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMinimum(Calendar.DAY_OF_MONTH));
    calendar.set(Calendar.HOUR_OF_DAY, calendar.getActualMinimum(Calendar.HOUR_OF_DAY));
    calendar.set(Calendar.MINUTE, calendar.getActualMinimum(Calendar.MINUTE));
    calendar.set(Calendar.SECOND, calendar.getActualMinimum(Calendar.SECOND));

    return calendar.getTime();
  }

  public static Date monthEnd(String year, String month) {
    Calendar calendar = Calendar.getInstance();

    calendar.set(Calendar.YEAR, Integer.valueOf(year));
    calendar.set(Calendar.MONTH, Integer.valueOf(month) - 1);
    calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
    calendar.set(Calendar.HOUR_OF_DAY, calendar.getActualMaximum(Calendar.HOUR_OF_DAY));
    calendar.set(Calendar.MINUTE, calendar.getActualMaximum(Calendar.MINUTE));
    calendar.set(Calendar.SECOND, calendar.getActualMaximum(Calendar.SECOND));

    return calendar.getTime();
  }

}
