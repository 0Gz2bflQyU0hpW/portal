package com.weibo.dip.scheduler.test;

import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.quartz.CronScheduleBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerUtils;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComputeFireTimesMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(ComputeFireTimesMain.class);

  public static void main(String[] args) throws Exception {
    Date now = DateFormatUtils.ISO_8601_EXTENDED_DATETIME_FORMAT.parse("2018-06-18T11:30:00");

    List<Date> fireTimes =
        TriggerUtils.computeFireTimesBetween(
            (OperableTrigger)
                TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ? *"))
                    .build(),
            null,
            DateUtils.addSeconds(now, -30),
            DateUtils.addSeconds(now, -0));

    for (Date fireTime : fireTimes) {
      LOGGER.info(DateFormatUtils.ISO_8601_EXTENDED_DATETIME_FORMAT.format(fireTime));
    }
  }
}
