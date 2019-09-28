package com.weibo.dip.baishanyundownload;

import com.google.common.base.Stopwatch;
import com.weibo.dip.baishanyundownload.domain.Domain;
import com.weibo.dip.baishanyundownload.domain.DomainService;
import com.weibo.dip.baishanyundownload.log.DomainLogDetail;
import com.weibo.dip.baishanyundownload.log.DomainLogDetailService;
import com.weibo.dip.data.platform.QuartzScheduler;
import com.weibo.dip.data.platform.commons.ClasspathProperties;
import com.weibo.dip.data.platform.commons.Symbols;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.redis.RedisClient;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Baishanyun download Main.
 *
 * @author yurun
 */
public class BaishanyunDownloadMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaishanyunDownloadMain.class);

  private static final FastDateFormat YYYY_MM_DD_HH_MM =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm");

  private static final String BAISHANYUN_DOWNLOAD_PROPERTIES = "baishanyundownload.properties";

  private static final ClasspathProperties CONF;

  private static final QuartzScheduler SCHEDULER;
  private static final DomainService DOMAIN_SERVICE;
  private static final DomainLogDetailService DOMAIN_LOG_DETAIL_SERVICE;
  private static final RedisClient REDIS_CLIENT;

  static {
    try {
      CONF = new ClasspathProperties(BAISHANYUN_DOWNLOAD_PROPERTIES);

      SCHEDULER = new QuartzScheduler();
      SCHEDULER.start();

      DOMAIN_SERVICE = new DomainService(CONF.getString("collector.token"));

      DOMAIN_LOG_DETAIL_SERVICE =
          new DomainLogDetailService(
              CONF.getString("collector.token"), CONF.getInt("collector.domain.log.fetch.threads"));

      REDIS_CLIENT =
          new RedisClient(
              CONF.getString("collector.redis.host"), CONF.getInt("collector.redis.port"));
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static class Collector implements Job {
    @Override
    public void execute(JobExecutionContext context) {
      Date scheduleTime = context.getScheduledFireTime();
      Date recentHour = DateUtils.addHours(scheduleTime, -CONF.getInt("collector.recent.hours"));

      try {
        Stopwatch stopwatch = Stopwatch.createStarted();

        List<Domain> domains = DOMAIN_SERVICE.getDomains();

        List<String> onlineDomains = new ArrayList<>();

        for (Domain domain : domains) {
          onlineDomains.add(domain.getDomain());
        }

        List<DomainLogDetail> domainLogDetails =
            DOMAIN_LOG_DETAIL_SERVICE.getLogs(onlineDomains, recentHour, scheduleTime);

        int queued = 0;

        for (DomainLogDetail domainLogDetail : domainLogDetails) {
          String alreadQueuedKey =
              CONF.getString("collector.redis.key.already.queued")
                  + Symbols.UNDERLINE
                  + domainLogDetail.getLogName();
          String alreadQueuedValue = domainLogDetail.getLogName();

          if (REDIS_CLIENT.setnx(alreadQueuedKey, alreadQueuedValue) == 1) {
            REDIS_CLIENT.expire(
                alreadQueuedKey, CONF.getInt("collector.redis.key.already.queued.expire.seconds"));

            Map<String, Object> data = new HashMap<>();

            data.put("Category", CONF.getString("collector.category"));
            data.put("FileName", domainLogDetail.getLogName());
            data.put("FileUrl", domainLogDetail.getUrl());
            data.put("Timestamp", YYYY_MM_DD_HH_MM.parse(domainLogDetail.getDate()).getTime());

            REDIS_CLIENT.rpush(CONF.getString("collector.redis.key.queue"), GsonUtil.toJson(data));

            LOGGER.info("get domain log: {}", domainLogDetail.getLogName());

            queued++;
          }
        }

        stopwatch.stop();

        LOGGER.info(
            "collect domain logs consume: {} ms, size: {}",
            stopwatch.elapsed(TimeUnit.MILLISECONDS),
            queued);
      } catch (Exception e) {
        LOGGER.error("collector execute error: {}", ExceptionUtils.getStackTrace(e));
      }
    }
  }

  /**
   * Main.
   *
   * @param args no param
   * @throws SchedulerException if quartz scheduler error
   */
  public static void main(String[] args) throws SchedulerException {
    String name = CONF.getString("collector.name");
    String cron = CONF.getString("collector.cron");
    Class<? extends Job> jobClass = Collector.class;
    JobDataMap jobData = new JobDataMap();

    SCHEDULER.schedule(name, cron, jobClass, jobData);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    SCHEDULER.shutdown();

                    REDIS_CLIENT.close();
                  } catch (Exception e) {
                    LOGGER.error("shutdown error: {}", ExceptionUtils.getStackTrace(e));
                  }
                }));
  }
}
