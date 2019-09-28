package com.weibo.dip.aliyundownload.util;

import com.weibo.dip.data.platform.commons.ClasspathProperties;
import com.weibo.dip.data.platform.redis.RedisClient;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aliyun already queued counter Main.
 *
 * @author yurun
 */
public class AlreadyQueuedCounter {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlreadyQueuedCounter.class);

  private static final String ALIYUN_DOWNLOAD_PROPERTIES = "aliyundownload.properties";

  private static final ClasspathProperties CONF;

  private static final RedisClient REDIS_CLIENT;

  static {
    try {
      CONF = new ClasspathProperties(ALIYUN_DOWNLOAD_PROPERTIES);

      REDIS_CLIENT =
          new RedisClient(
              CONF.getString("collector.redis.host"), CONF.getInt("collector.redis.port"));
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Main.
   *
   * @param args no param
   */
  public static void main(String[] args) {
    Set<String> alreadyQueuedLogs =
        REDIS_CLIENT.keys(CONF.getString("collector.redis.key.already.queued") + "*");

    LOGGER.info(
        "already queued length: {}",
        Objects.nonNull(alreadyQueuedLogs) ? alreadyQueuedLogs.size() : 0);
  }
}
