package com.weibo.dip.baishanyundownload.util;

import com.weibo.dip.data.platform.commons.ClasspathProperties;
import com.weibo.dip.data.platform.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Baishanyun queue counter Main.
 *
 * @author yurun
 */
public class QueueCounter {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueueCounter.class);

  private static final String BAISHANYUN_DOWNLOAD_PROPERTIES = "conf/baishanyundownload.properties";

  private static final ClasspathProperties CONF;

  private static final RedisClient REDIS_CLIENT;

  static {
    try {
      CONF = new ClasspathProperties(BAISHANYUN_DOWNLOAD_PROPERTIES);

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
    LOGGER.info("queue length: {}", REDIS_CLIENT.llen(CONF.getString("collector.redis.key.queue")));

    REDIS_CLIENT.close();
  }
}
