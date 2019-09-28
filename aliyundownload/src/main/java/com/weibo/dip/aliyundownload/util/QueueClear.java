package com.weibo.dip.aliyundownload.util;

import com.weibo.dip.data.platform.commons.ClasspathProperties;
import com.weibo.dip.data.platform.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aliyun queue clear Main.
 *
 * @author yurun
 */
public class QueueClear {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueueClear.class);

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
    REDIS_CLIENT.del(CONF.getString("collector.redis.key.queue"));

    LOGGER.info("queued cleared");

    REDIS_CLIENT.close();
  }
}
