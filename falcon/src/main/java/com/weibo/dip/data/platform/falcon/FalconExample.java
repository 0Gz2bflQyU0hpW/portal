package com.weibo.dip.data.platform.falcon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Falcon Example.
 *
 * @author yurun
 */
public class FalconExample {
  private static final Logger LOGGER = LoggerFactory.getLogger(FalconExample.class);

  /**
   * Main.
   *
   * @param args no params
   */
  public static void main(String[] args) throws Exception {
    LOGGER.debug("debug ...");
    LOGGER.info("info ...");
    LOGGER.warn("warn ...");
    LOGGER.error("error ...");

    int count = 0;

    while (++count < 100) {
      LOGGER.info("times: {}", System.currentTimeMillis());

      Thread.sleep(3 * 60 * 1000);
    }
  }
}
