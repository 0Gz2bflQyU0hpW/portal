package com.weibo.dip.aliyundownload.util;

import com.weibo.dip.data.platform.commons.ClasspathProperties;
import com.weibo.dip.data.platform.redis.RedisClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aliyun queue poper Main.
 *
 * @author yurun
 */
public class QueuePoper {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueuePoper.class);

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
    while (true) {
      String value = REDIS_CLIENT.lpop(CONF.getString("collector.redis.key.queue"));
      if (StringUtils.isEmpty(value)) {
        break;
      }

      LOGGER.info(value);
    }

    REDIS_CLIENT.close();
  }
}
