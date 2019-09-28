package com.weibo.dip.aliyundownload;

import com.google.common.base.Stopwatch;
import com.weibo.dip.aliyundownload.domain.Domain;
import com.weibo.dip.aliyundownload.domain.DomainService;
import com.weibo.dip.aliyundownload.log.DomainLogDetail;
import com.weibo.dip.aliyundownload.log.DomainLogDetailService;
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
import org.apache.commons.lang3.time.FastTimeZone;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aliyun download Main.
 *
 * @author yurun
 */
public class AliyunDownloadMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(AliyunDownloadMain.class);

  private static final FastDateFormat ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z =
      FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ssZZ", FastTimeZone.getGmtTimeZone());

  private static final String ALIYUN_DOWNLOAD_PROPERTIES = "aliyundownload.properties";

  private static final ClasspathProperties CONF;

  private static final QuartzScheduler SCHEDULER;
  private static final DomainService DOMAIN_SERVICE;
  private static final DomainLogDetailService DOMAIN_LOG_DETAIL_SERVICE;
  private static final RedisClient REDIS_CLIENT;

  static {
    try {
      CONF = new ClasspathProperties(ALIYUN_DOWNLOAD_PROPERTIES);

      SCHEDULER = new QuartzScheduler();
      SCHEDULER.start();

      DOMAIN_SERVICE =
          new DomainService(
              CONF.getString("collector.accesskey"), CONF.getString("collector.accesskeysecret"));

      DOMAIN_LOG_DETAIL_SERVICE =
          new DomainLogDetailService(
              CONF.getString("collector.accesskey"),
              CONF.getString("collector.accesskeysecret"),
              CONF.getInt("collector.domain.log.fetch.threads"));

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
          // Aliyun Rest API Exception
          //          if (domain.getDomainStatus().equals("online")) {
          //            onlineDomains.add(domain.getDomainName());
          //          }
          onlineDomains.add(domain.getDomainName());
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
            data.put("FileUrl", "http://" + domainLogDetail.getLogPath());
            data.put(
                "Timestamp",
                ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z.parse(domainLogDetail.getStartTime()).getTime());

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
