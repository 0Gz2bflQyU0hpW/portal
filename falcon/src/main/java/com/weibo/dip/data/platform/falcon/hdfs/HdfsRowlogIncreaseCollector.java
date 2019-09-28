package com.weibo.dip.data.platform.falcon.hdfs;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.QuartzScheduler;
import com.weibo.dip.data.platform.commons.Symbols;
import com.weibo.dip.data.platform.commons.metric.Metric;
import com.weibo.dip.data.platform.commons.metric.MetricStore;
import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nyx hdfs rawlog size monitor.
 *
 * @author yurun
 */
public class HdfsRowlogIncreaseCollector {
  public static final Logger LOGGER = LoggerFactory.getLogger(HdfsRowlogIncreaseCollector.class);

  public static final String HDFS_BASE = "/user/hdfs/rawlog";

  public static final FastDateFormat HDFS_DATE_FORMAT = FastDateFormat.getInstance("yyyy_MM_dd/HH");

  /**
   * Get category names.
   *
   * @return category names
   * @throws Exception if access hdfs error
   */
  public static List<String> getCategories() throws Exception {
    return HDFSUtil.listDirs(HDFS_BASE, false)
        .stream()
        .map(Path::getName)
        .collect(Collectors.toList());
  }

  /**
   * Get category size in specified time range.
   *
   * @param category category name
   * @param startTime start time(include)
   * @param endTime end time(include)
   * @return category size
   * @throws Exception if access hdfs error
   */
  public static long getSize(String category, Date startTime, Date endTime) throws Exception {
    long length = 0;

    Date time = startTime;

    while (time.compareTo(endTime) <= 0) {
      String categoryPath =
          HDFS_BASE + Symbols.SLASH + category + Symbols.SLASH + HDFS_DATE_FORMAT.format(time);

      ContentSummary summary = HDFSUtil.summary(categoryPath);
      if (Objects.nonNull(summary)) {
        LOGGER.debug("Dir {} length: {}", categoryPath, summary.getLength());

        length += summary.getLength();
      } else {
        LOGGER.debug("Dir {} does not exist", categoryPath);
      }

      time = DateUtils.addHours(time, 1);
    }

    return length;
  }

  private static class Record {
    private String category;
    private Date startTime;
    private Date endTime;
    private long size;

    public Record(String category, Date startTime, Date endTime, long size) {
      this.category = category;
      this.startTime = startTime;
      this.endTime = endTime;
      this.size = size;
    }

    public Date getStartTime() {
      return startTime;
    }

    public long getSize() {
      return size;
    }
  }

  @PersistJobDataAfterExecution
  @DisallowConcurrentExecution
  public static class Collector implements Job {

    private String businessName = "dip-falcon-hdfs-rawlog-increase";
    private String categoryName = "category";
    private String sizeName = "size";

    @Override
    public void execute(JobExecutionContext context) {
      Date schedule = context.getScheduledFireTime();

      Date now = DateUtils.truncate(schedule, Calendar.HOUR);

      Date previousDay = DateUtils.addDays(now, -1);

      JobDataMap history = context.getJobDetail().getJobDataMap();

      try {
        List<String> categories = getCategories();

        Preconditions.checkState(
            CollectionUtils.isNotEmpty(categories), "categories cat not be empty");

        for (String category : categories) {
          if (history.containsKey(category)) {
            Record historyRecord = (Record) history.get(category);

            long historySize = historyRecord.getSize();
            long nowSize = getSize(category, historyRecord.getStartTime(), now);
            long size = nowSize - historySize;

            MetricStore.store(
                businessName,
                schedule.getTime(),
                Metric.newEntity(categoryName, category),
                Metric.newEntity(sizeName, size));

            LOGGER.info(
                "category {}, now size: {}, history size: {}, increase size: {}",
                category,
                nowSize,
                historySize,
                size);
          }

          history.put(
              category,
              new Record(category, previousDay, now, getSize(category, previousDay, now)));
        }

      } catch (Exception e) {
        LOGGER.error("collector execute error: {}", ExceptionUtils.getStackTrace(e));
      }
    }
  }

  /**
   * HDFS rawlog increase size collector.
   *
   * @param args no params
   * @throws Exception if schedule error or access hdfs error
   */
  public static void main(String[] args) throws Exception {
    QuartzScheduler scheduler = new QuartzScheduler();

    scheduler.start();

    String name = "hdfs_rawlog_increase_collector";
    String cron = "0 0/5 * * * ?";
    Class<? extends Job> jobClass = Collector.class;
    JobDataMap jobData = new JobDataMap();

    scheduler.schedule(name, cron, jobClass, jobData);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    scheduler.shutdown();
                  } catch (SchedulerException e) {
                    LOGGER.error("scheduler shutdown error: {}", ExceptionUtils.getStackTrace(e));
                  }
                }));
  }
}
