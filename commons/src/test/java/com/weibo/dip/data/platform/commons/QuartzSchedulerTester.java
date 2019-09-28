package com.weibo.dip.data.platform.commons;

import com.weibo.dip.data.platform.QuartzScheduler;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author yurun */
public class QuartzSchedulerTester {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuartzSchedulerTester.class);

  public static class HelloJob implements Job {
    @Override
    public void execute(JobExecutionContext context) {
      LOGGER.info("hello {}", context.getScheduledFireTime());
    }
  }

  public static void main(String[] args) throws Exception {
    QuartzScheduler scheduler = new QuartzScheduler();

    scheduler.start();

    scheduler.schedule("hello", "0 0/1 * * * ?", HelloJob.class);

    Thread.sleep(3 * 60 * 1000);

    scheduler.shutdown();
  }
}
