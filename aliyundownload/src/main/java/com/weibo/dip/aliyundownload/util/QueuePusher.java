package com.weibo.dip.aliyundownload.util;

import com.weibo.dip.aliyundownload.domain.Domain;
import com.weibo.dip.aliyundownload.domain.DomainService;
import com.weibo.dip.aliyundownload.log.DomainLogDetail;
import com.weibo.dip.aliyundownload.log.DomainLogDetailService;
import com.weibo.dip.data.platform.commons.ClasspathProperties;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.redis.RedisClient;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.lang3.time.FastTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queue pusher.
 *
 * @author yurun
 */
public class QueuePusher {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueuePusher.class);

  private static final String ALIYUN_DOWNLOAD_PROPERTIES = "aliyundownload.properties";

  private static final FastDateFormat YYYYMMDD = FastDateFormat.getInstance("yyyyMMdd");
  private static final FastDateFormat YYYY_MM_DD_HH_MM_SS =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
  private static final FastDateFormat ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z =
      FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ssZZ", FastTimeZone.getGmtTimeZone());

  /**
   * Main.
   *
   * @param args args[0] yyyyMMdd
   */
  public static void main(String[] args) {
    String day = YYYYMMDD.format(new Date());

    try {
      if (ArrayUtils.isNotEmpty(args)) {
        day = args[0];
      }
      LOGGER.info("day: {}", day);

      Calendar calendar = Calendar.getInstance();

      calendar.setTime(YYYYMMDD.parse(day));

      calendar.set(Calendar.HOUR_OF_DAY, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);

      String startTime = YYYY_MM_DD_HH_MM_SS.format(calendar.getTime());
      LOGGER.info("startTime: {}", startTime);

      calendar.add(Calendar.DAY_OF_YEAR, 1);
      String endTime = YYYY_MM_DD_HH_MM_SS.format(calendar.getTime());
      LOGGER.info("endTime: {}", endTime);

      ClasspathProperties conf = new ClasspathProperties(ALIYUN_DOWNLOAD_PROPERTIES);

      String accessKeyId = conf.getString("collector.accesskey");
      String accessKeySecret = conf.getString("collector.accesskeysecret");

      DomainService domainService = new DomainService(accessKeyId, accessKeySecret);

      List<Domain> domains = domainService.getDomains();

      LOGGER.info("domains: {}", domains.size());

      Set<String> set = new HashSet<>();

      for (Domain domain : domains) {
        set.add(domain.getDomainName());
      }

      LOGGER.info("domains set: {}", set.size());

      int threads = conf.getInt("collector.domain.log.fetch.threads");

      DomainLogDetailService domainLogService =
          new DomainLogDetailService(accessKeyId, accessKeySecret, threads);

      List<DomainLogDetail> domainLogDetails =
          domainLogService.getLogs(
              domains.stream().map(Domain::getDomainName).collect(Collectors.toList()),
              startTime,
              endTime);

      LOGGER.info("domain logs: {}", domainLogDetails.size());

      String host = conf.getString("collector.redis.host");
      int port = conf.getInt("collector.redis.port");

      String category = conf.getString("collector.category");

      String queue = conf.getString("collector.redis.key.queue");

      RedisClient redisClient = new RedisClient(host, port);

      try {
        for (DomainLogDetail domainLogDetail : domainLogDetails) {
          Map<String, Object> data = new HashMap<>();

          data.put("Category", category);
          data.put("FileName", domainLogDetail.getLogName());
          data.put("FileUrl", "http://" + domainLogDetail.getLogPath());
          data.put(
              "Timestamp",
              ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z.parse(domainLogDetail.getStartTime()).getTime());

          redisClient.rpush(queue, GsonUtil.toJson(data));
        }
      } finally {
        redisClient.close();
      }
    } catch (Exception e) {
      LOGGER.error("queue push error: {}", ExceptionUtils.getStackTrace(e));
    }
  }
}
