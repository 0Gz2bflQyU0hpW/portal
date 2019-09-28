package com.weibo.dip.baishanyundownload.util;

import com.weibo.dip.baishanyundownload.domain.Domain;
import com.weibo.dip.baishanyundownload.domain.DomainService;
import com.weibo.dip.baishanyundownload.log.DomainLogDetail;
import com.weibo.dip.baishanyundownload.log.DomainLogDetailService;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queue pusher.
 *
 * @author yurun
 */
public class QueuePusher {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueuePusher.class);

  private static final String BAISHANYUN_DOWNLOAD_PROPERTIES = "conf/baishanyundownload.properties";

  private static final FastDateFormat YYYYMMDD = FastDateFormat.getInstance("yyyyMMdd");
  private static final FastDateFormat YYYY_MM_DD_HH_MM =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm");

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

      Date startTime = calendar.getTime();
      LOGGER.info("startTime: {}", startTime);

      calendar.add(Calendar.DAY_OF_YEAR, 1);
      Date endTime = calendar.getTime();
      LOGGER.info("endTime: {}", endTime);

      ClasspathProperties conf = new ClasspathProperties(BAISHANYUN_DOWNLOAD_PROPERTIES);

      String token = conf.getString("collector.token");

      DomainService domainService = new DomainService(token);

      List<Domain> domains = domainService.getDomains();

      LOGGER.info("domains: {}", domains.size());

      Set<String> set = new HashSet<>();

      for (Domain domain : domains) {
        set.add(domain.getDomain());
      }

      LOGGER.info("domains set: {}", set.size());

      int threads = conf.getInt("collector.domain.log.fetch.threads");

      DomainLogDetailService domainLogService = new DomainLogDetailService(token, threads);

      List<DomainLogDetail> domainLogDetails =
          domainLogService.getLogs(
              domains.stream().map(Domain::getDomain).collect(Collectors.toList()),
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
          data.put("FileUrl", domainLogDetail.getUrl());
          data.put("Timestamp", YYYY_MM_DD_HH_MM.parse(domainLogDetail.getDate()).getTime());

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
