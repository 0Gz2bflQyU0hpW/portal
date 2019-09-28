package com.weibo.dip.aliyundownload.util;

import com.weibo.dip.aliyundownload.domain.Domain;
import com.weibo.dip.aliyundownload.domain.DomainService;
import com.weibo.dip.aliyundownload.log.DomainLogDetail;
import com.weibo.dip.aliyundownload.log.DomainLogDetailService;
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
import org.apache.commons.lang3.time.FastTimeZone;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aliyun log count checker.
 *
 * @author yurun
 */
public class






LogCountChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogCountChecker.class);

  private static final String ALIYUN_DOWNLOAD_PROPERTIES = "aliyundownload.properties";

  private static final FastDateFormat YYYYMMDD = FastDateFormat.getInstance("yyyyMMdd");
  private static final FastDateFormat YYYY_MM_DD = FastDateFormat.getInstance("yyyy_MM_dd");
  private static final FastDateFormat YYYY_MM_DD_HH_MM_SS =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
  private static final FastDateFormat ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z =
      FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ssZZ", FastTimeZone.getGmtTimeZone());

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

      ClasspathProperties conf = new ClasspathProperties(ALIYUN_DOWNLOAD_PROPERTIES);

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

      String startTime = YYYY_MM_DD_HH_MM_SS.format(calendar.getTime());
      LOGGER.info("startTime: {}", startTime);

      calendar.add(Calendar.DAY_OF_YEAR, 1);
      String endTime = YYYY_MM_DD_HH_MM_SS.format(calendar.getTime());
      LOGGER.info("endTime: {}", endTime);

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

      if (hdfsLogs.size() != domainLogDetails.size()) {
        String service = "Databus";
        String subService = "AliyunDownload";
        String subject =
            "hdfs logs("
                + hdfsLogs.size()
                + ") inconsistent with aliyun logs("
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
                    data.put("FileUrl", "http://" + domainLogDetail.getLogPath());
                    try {
                      data.put(
                          "Timestamp",
                          ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z
                              .parse(domainLogDetail.getStartTime())
                              .getTime());
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
