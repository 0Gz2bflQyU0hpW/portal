package com.weibo.dip.warehouse;

import com.weibo.dip.data.platform.commons.hive.HiveServer2Client;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive sql executor.
 *
 * @author yurun
 */
public class HiveSqlExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveSqlExecutor.class);

  private static final String HIVE_SERVER = "jdbc:hive2://10.77.114.140:10001";

  private static final String SCHEDULE_TIME = "\\$SCHEDULETIME";

  private static String analyzeSql(String sql, long scheduleTime) {
    sql = sql.replaceAll(SCHEDULE_TIME, String.valueOf(scheduleTime));

    return sql;
  }

  /**
   * Main.
   *
   * @param args args[0] sql file
   */
  public static void main(String[] args) {
    long scheduleTime = System.currentTimeMillis() / 1000;

    if (ArrayUtils.isEmpty(args)) {
      LOGGER.error("HiveSQLExecutor params: sqlFile [scheduleTime].");

      return;
    }

    File sqlFile = FileUtils.getFile(args[0]);
    if (!sqlFile.exists()) {
      LOGGER.error("sql file {} not exist", sqlFile.exists());

      return;
    }

    if (args.length > 1) {
      try {
        scheduleTime = Long.valueOf(args[1]);
      } catch (NumberFormatException e) {
        LOGGER.error("scheduleTime {} is not a number", args[1]);

        return;
      }
    }

    String[] sqls;

    try {
      List<String> lines = FileUtils.readLines(sqlFile);
      if (CollectionUtils.isEmpty(lines)) {
        LOGGER.warn("sql file {} is empty", sqlFile);

        return;
      }

      sqls =
          StringUtils.join(
                  lines.stream().filter(line -> !line.isEmpty()).collect(Collectors.toList()), "\n")
              .split(";", -1);
    } catch (IOException e) {
      LOGGER.error("sql file {} read error: {}", ExceptionUtils.getStackTrace(e));

      return;
    }

    HiveServer2Client client = null;

    try {
      client = new HiveServer2Client(HIVE_SERVER);

      for (String sql : sqls) {
        sql = sql.trim();
        if (sql.isEmpty()) {
          continue;
        }

        sql = analyzeSql(sql, scheduleTime);

        LOGGER.info("execute sql: {}", sql);
        client.execute(sql);
      }
    } catch (Exception e) {
      LOGGER.error("execute sql error: {}", ExceptionUtils.getStackTrace(e));
    } finally {
      if (Objects.nonNull(client)) {
        client.close();
      }
    }
  }
}
