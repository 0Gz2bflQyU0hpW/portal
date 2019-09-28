package com.weibo.dip.warehouse.engine;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.commons.ClasspathProperties;
import com.weibo.dip.data.platform.commons.hive.HiveServer2Client;
import java.util.Objects;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * App engine.
 *
 * @author yurun
 */
public class AppEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(AppEngine.class);

  private static final String SCHEDULE_TIME = "\\$SCHEDULETIME";

  private static String analyzeSql(String sql, long scheduleTime) {
    sql = sql.replaceAll(SCHEDULE_TIME, String.valueOf(scheduleTime));

    return sql;
  }

  /**
   * Main.
   *
   * @param args [name, scheduleTime]
   */
  public static void main(String[] args) {
    if (ArrayUtils.isEmpty(args)) {
      LOGGER.warn("AppEngine args[name, scheduleTime] is empty");

      System.exit(-1);
    }

    try {
      String name = args[0];
      Preconditions.checkState(StringUtils.isNotEmpty(name), "name is empty");

      long scheduleTime = Long.valueOf(args[1]);
      scheduleTime = scheduleTime / 1000;

      ClasspathProperties properties = new ClasspathProperties("engine.properties");

      SqlFetcher fetcher = new SqlFetcher(properties);

      String[] sqls = fetcher.fetch(name);
      if (ArrayUtils.isEmpty(sqls)) {
        LOGGER.warn("{}'s sqls is empty", name);

        return;
      }

      String hiveUrl = properties.getString("hive.url");

      HiveServer2Client client = null;

      try {
        client = new HiveServer2Client(hiveUrl);

        for (String sql : sqls) {
          sql = sql.trim();
          if (sql.isEmpty()) {
            continue;
          }

          sql = analyzeSql(sql, scheduleTime);

          LOGGER.info("execute sql: {}", sql);
          client.execute(sql);
        }
      } finally {
        if (Objects.nonNull(client)) {
          client.close();
        }
      }
    } catch (Exception e) {
      LOGGER.error("app engine run error: {}", ExceptionUtils.getFullStackTrace(e));

      System.exit(-1);
    }
  }
}
