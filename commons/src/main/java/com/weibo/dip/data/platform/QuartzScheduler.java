package com.weibo.dip.data.platform;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.commons.lang.StringUtils;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

/**
 * Quartz scheduler.
 *
 * @author yurun
 */
public class QuartzScheduler {
  private Scheduler scheduler;

  public QuartzScheduler() throws SchedulerException {
    scheduler = StdSchedulerFactory.getDefaultScheduler();
  }

  public void start() throws SchedulerException {
    scheduler.start();
  }

  public void schedule(String name, String cron, Class<? extends Job> jobClass)
      throws IllegalStateException, SchedulerException {
    schedule(name, cron, jobClass, null);
  }

  /**
   * Schedule a job.
   *
   * @param name jobname
   * @param cron cron expression, example: 0 0/5 * * * ?(every 5 minutes)
   * @param jobClass job class implements interface Job
   * @param jobData job data
   * @throws IllegalStateException if scheduler isn't started or params aren't specified
   * @throws SchedulerException if schedule job failure
   */
  public void schedule(String name, String cron, Class<? extends Job> jobClass, JobDataMap jobData)
      throws IllegalStateException, SchedulerException {
    Preconditions.checkState(scheduler.isStarted(), "scheduler must be started");

    Preconditions.checkState(StringUtils.isNotEmpty(name), "name must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(cron), "cron must be specified");
    Preconditions.checkState(Objects.nonNull(jobClass), "jobClass must be specified");

    JobBuilder jobBuilder = JobBuilder.newJob(jobClass).withIdentity(name);

    if (Objects.nonNull(jobData)) {
      jobBuilder = jobBuilder.usingJobData(jobData);
    }

    JobDetail jobDetail = jobBuilder.build();

    Trigger trigger =
        TriggerBuilder.newTrigger()
            .withIdentity(name)
            .withSchedule(CronScheduleBuilder.cronSchedule(cron))
            .build();

    scheduler.scheduleJob(jobDetail, trigger);
  }

  public void shutdown() throws SchedulerException {
    scheduler.shutdown(true);
  }
}
