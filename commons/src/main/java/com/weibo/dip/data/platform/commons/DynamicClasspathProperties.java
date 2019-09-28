package com.weibo.dip.data.platform.commons;

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dynamic load key/value from file in classpath.
 *
 * @author yurun
 */
public class DynamicClasspathProperties extends ClasspathProperties {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicClasspathProperties.class);

  private static final long DEFAULT_INTERVAL = 60 * 1000;

  private File file;
  private long timestamp = -1;

  private long interval;

  /**
   * Construct properties with specified fileName and default time interval.
   *
   * @param fileName classpath: file name
   * @throws Exception if classpath:fileName does not exist, or load properties error
   */
  public DynamicClasspathProperties(String fileName) throws Exception {
    this(fileName, DEFAULT_INTERVAL);
  }

  private class Loader extends Thread {
    @Override
    public void run() {
      while (!isInterrupted()) {
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          LOGGER.error("loader sleep, but interrupted");

          interrupt();
        }

        try {
          load();
        } catch (Exception e) {
          LOGGER.error("load properties error: {}", ExceptionUtils.getFullStackTrace(e));
        }
      }
    }
  }

  /**
   * Construct properties with specified fileName and time interval.
   *
   * @param fileName classpath: file name
   * @param interval dynamic load time interval
   * @throws Exception if classpath:fileName does not exist, or load properties error
   */
  public DynamicClasspathProperties(String fileName, long interval) throws Exception {
    this.interval = interval;

    URL resource = DynamicClasspathProperties.class.getClassLoader().getResource(fileName);
    Preconditions.checkState(Objects.nonNull(resource), "classpath: {} does not exist", fileName);

    file = new File(resource.toURI());

    properties = new Properties();

    load();

    Loader loader = new Loader();

    loader.setDaemon(true);
    loader.start();
  }

  private void load() throws Exception {
    long lastModified = file.lastModified();

    if (lastModified > timestamp) {
      BufferedReader reader = null;

      try {
        reader =
            new BufferedReader(
                new InputStreamReader(new FileInputStream(file), CharEncoding.UTF_8));

        properties.load(reader);

        timestamp = lastModified;

        LOGGER.info("load properties to timestamp {}", timestamp);
      } finally {
        if (Objects.nonNull(reader)) {
          reader.close();
        }
      }
    }
  }
}
