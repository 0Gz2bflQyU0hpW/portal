package com.weibo.dip.warehouse;

import com.weibo.dip.data.platform.QuartzScheduler;
import com.weibo.dip.data.platform.commons.hive.HiveServer2Client;
import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.fs.Path;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive table partition mount.
 *
 * @author yurun
 */
public class HiveTablePartitionMountMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveTablePartitionMountMain.class);

  private static final String NAME = "hive_table_partition_mount";
  private static final String CRON = "0 10 * * * ? *";

  private static final String HIVESERVER2_URL = "jdbc:hive2://10.77.114.141:10001";

  private static final String DB = "warehouse";

  private static final String DIRTIME = "dirtime";

  private static final String DATASET = "dataset";
  private static final String RAWLOG_DIR = "/user/hdfs/rawlog/";

  private static final FastDateFormat RAWLOG_DATE_FORMAT =
      FastDateFormat.getInstance("yyyy_MM_dd/HH");
  private static final FastDateFormat WAREHOUSE_DATE_FORMAT =
      FastDateFormat.getInstance("yyyyMMddHH");

  public static class Mounter implements Job {
    @Override
    public void execute(JobExecutionContext context) {
      HiveServer2Client client = null;

      try {
        client = new HiveServer2Client(HIVESERVER2_URL);

        List<String> tables = client.showTables(DB);

        for (String table : tables) {
          LOGGER.info("table: " + table);

          String dataset = client.getTblPropertyValue(DB, table, DATASET);
          if (StringUtils.isEmpty(dataset)) {
            LOGGER.info("table {}'s dataset value is empty, skip", table);
            continue;
          }

          /*
          Add partition
           */
          List<Path> dayDirs = HDFSUtil.listDirs(RAWLOG_DIR + dataset, false);
          if (CollectionUtils.isEmpty(dayDirs)) {
            LOGGER.info("table {} day dirs is empty, skip", table);
            continue;
          }

          for (Path dayDir : dayDirs) {
            LOGGER.info("day dir: {}", dayDir.toString());

            String day = dayDir.getName();

            List<Path> hourDirs = HDFSUtil.listDirs(dayDir, false);
            if (CollectionUtils.isEmpty(hourDirs)) {
              LOGGER.info("day dir {} has no hour dirs, skip", dayDir);
              continue;
            }

            for (Path hourDir : hourDirs) {
              LOGGER.info("hour dir: {}", hourDir.toString());

              String hour = hourDir.getName();

              String dirtime =
                  WAREHOUSE_DATE_FORMAT.format(RAWLOG_DATE_FORMAT.parse(day + "/" + hour));
              LOGGER.info("dirtime: {}", dirtime);

              Map<String, String> specs = Collections.singletonMap(DIRTIME, dirtime);

              if (!client.existPartition(DB, table, specs)) {
                client.addPartition(DB, table, specs, hourDir.toString());
                LOGGER.info("partition {} not exist, add", DB, table, specs);
              } else {
                LOGGER.info("partiton {} existed", specs);
              }
            }
          }

          LOGGER.info("table {} add partitions finished", table);

          /*
          Remove partiton
           */
          List<Map<String, String>> partitons = client.showPartitions(DB, table);
          if (CollectionUtils.isEmpty(partitons)) {
            LOGGER.info("table {} partitions is empty, skip", table);
            continue;
          }

          for (Map<String, String> partition : partitons) {
            String dirtime = partition.get(DIRTIME);

            String dirpath =
                RAWLOG_DIR
                    + dataset
                    + "/"
                    + RAWLOG_DATE_FORMAT.format(WAREHOUSE_DATE_FORMAT.parse(dirtime));

            if (!HDFSUtil.exist(dirpath)) {
              client.dropPartition(DB, table, partition);
              LOGGER.info("partition {}'s hdfs dir {} deleted, drop", dirpath, dirtime);
            } else {
              LOGGER.info("partition {}'s hdfs dir {} exists, skip", partition, dirpath);
            }
          }

          LOGGER.info("table {} remove partitions finished", table);
        }

        LOGGER.info("mount finished");
      } catch (Exception e) {
        LOGGER.error("mount hive table partition error: {}", ExceptionUtils.getFullStackTrace(e));
      } finally {
        if (Objects.nonNull(client)) {
          client.close();
        }
      }
    }
  }

  /**
   * Main.
   *
   * @param args no params
   * @throws Exception if run error
   */
  public static void main(String[] args) throws Exception {
    Class<? extends Job> jobClass = Mounter.class;

    QuartzScheduler scheduler = new QuartzScheduler();
    scheduler.start();

    scheduler.schedule(NAME, CRON, jobClass);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    scheduler.shutdown();
                  } catch (Exception e) {
                    LOGGER.error(
                        "scheduler shutdown error: {}", ExceptionUtils.getFullStackTrace(e));
                  }
                }));
  }
}
