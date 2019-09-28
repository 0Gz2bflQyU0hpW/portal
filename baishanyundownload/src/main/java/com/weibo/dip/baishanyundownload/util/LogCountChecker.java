package com.weibo.dip.baishanyundownload.util;

import com.weibo.dip.baishanyundownload.domain.Domain;
import com.weibo.dip.baishanyundownload.domain.DomainService;
import com.weibo.dip.baishanyundownload.log.DomainLogDetail;
import com.weibo.dip.baishanyundownload.log.DomainLogDetailService;
import com.weibo.dip.data.platform.commons.ClasspathProperties;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import com.weibo.dip.data.platform.commons.util.WatchAlert;
import com.weibo.dip.data.platform.redis.RedisClient;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Baishanyun log count checker.
 *
 * @author yurun
 */
public class LogCountChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogCountChecker.class);

  private static final String BAISHANYUN_DOWNLOAD_PROPERTIES = "conf/baishanyundownload.properties";

  private static final FastDateFormat YYYYMMDD = FastDateFormat.getInstance("yyyyMMdd");
  private static final FastDateFormat YYYY_MM_DD = FastDateFormat.getInstance("yyyy_MM_dd");
  private static final FastDateFormat YYYY_MM_DD_HH_MM =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm");

  private static final String HDFS_RAWLOG = "/user/hdfs/rawlog/";

  private static String getYestoday() {
    Calendar calendar = Calendar.getInstance();

    calendar.setTime(new Date());

    calendar.add(Calendar.DAY_OF_YEAR, -1);

    return YYYYMMDD.format(calendar.getTime());
  }

  /**
   * Begint to check.
   *
   * @param args no args
   */
  public static void main(String[] args) {
    String day = getYestoday();

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

      ClasspathProperties conf = new ClasspathProperties(BAISHANYUN_DOWNLOAD_PROPERTIES);

      String category = conf.getString("collector.category");

      String hdfsPath = HDFS_RAWLOG + category + "/" + YYYY_MM_DD.format(calendar.getTime());

      LOGGER.info("hdfs path: {}", hdfsPath);

      Set<String> hdfsLogs = new HashSet<>();

      List<Path> files =
          HDFSUtil.listFiles(
              hdfsPath,
              true,
              p -> {
                String name = p.getName();
                return !name.startsWith("_") && !name.startsWith(".");
              });

      if (CollectionUtils.isNotEmpty(files)) {
        files.forEach(file -> hdfsLogs.add(file.getName().replace(".log", ".gz")));
      }

      LOGGER.info("hdfs logs: {}", hdfsLogs.size());

      String startTime = YYYY_MM_DD_HH_MM.format(calendar.getTime());
      LOGGER.info("startTime: {}", startTime);

      calendar.add(Calendar.DAY_OF_YEAR, 1);
      String endTime = YYYY_MM_DD_HH_MM.format(calendar.getTime());
      LOGGER.info("endTime: {}", endTime);

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

      if (hdfsLogs.size() != domainLogDetails.size()) {
        String service = "Databus";
        String subService = "BaishanyunDownload";
        String subject =
            "hdfs logs("
                + hdfsLogs.size()
                + ") inconsistent with baishanyun logs("
                + domainLogDetails.size()
                + ")";
        String content = "";
        String[] tos = {"DIP_ALL"};

        WatchAlert.alert(service, subService, subject, content, tos);

        String host = conf.getString("collector.redis.host");
        int port = conf.getInt("collector.redis.port");

        String queue = conf.getString("collector.redis.key.queue");

        RedisClient client = new RedisClient(host, port);

        try {
          domainLogDetails
              .stream()
              .filter(domainLogDetail -> !hdfsLogs.contains(domainLogDetail.getLogName()))
              .forEach(
                  domainLogDetail -> {
                    Map<String, Object> data = new HashMap<>();

                    data.put("Category", category);
                    data.put("FileName", domainLogDetail.getLogName());
                    data.put("FileUrl", domainLogDetail.getUrl());
                    try {
                      data.put(
                          "Timestamp", YYYY_MM_DD_HH_MM.parse(domainLogDetail.getDate()).getTime());
                    } catch (ParseException e) {
                      return;
                    }

                    client.rpush(queue, GsonUtil.toJson(data));

                    LOGGER.info("push {} into redis", domainLogDetail);
                  });
        } finally {
          client.close();
        }
      } else {
        LOGGER.info("hdfs logs == aliyun logs");
      }
    } catch (Exception e) {
      LOGGER.error("log count check error: {}", ExceptionUtils.getStackTrace(e));
    }
  }
}
