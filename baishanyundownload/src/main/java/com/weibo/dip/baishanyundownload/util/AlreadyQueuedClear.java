package com.weibo.dip.baishanyundownload.util;

import com.weibo.dip.data.platform.commons.ClasspathProperties;
import com.weibo.dip.data.platform.redis.RedisClient;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Baishanyun already queued clear Main.
 *
 * @author yurun
 */
public class AlreadyQueuedClear {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlreadyQueuedClear.class);

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
    Set<String> alreadyQueuedLogs =
        REDIS_CLIENT.keys(CONF.getString("collector.redis.key.already.queued") + "*");

    REDIS_CLIENT.del(alreadyQueuedLogs.toArray(new String[0]));

    LOGGER.info("already queued cleared");

    REDIS_CLIENT.close();
  }
}
