package com.weibo.dip.databus.core;

import com.google.common.base.Preconditions;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by yurun on 17/8/31.
 */
public class Metric implements Configurable, Lifecycle {

  private static final Logger LOGGER = LoggerFactory.getLogger(Metric.class);

  private static final String METRIC_PERSIST_INTERVAL = "metric.persist.interval";
  private static final String METRIC_PERSIST_CLASS = "metric.persist.class";
  private static final Metric METRIC = new Metric();
  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private Map<String, AtomicLong> metrics = new HashMap<>();
  private Saver saver;
  private long persistInterval;
  private Persist persister;

  private Metric() {

  }

  public static Metric getInstance() {
    return METRIC;
  }

  @Override
  public void setConf(Configuration conf) throws Exception {
    String persistIntervalStr = conf.get(METRIC_PERSIST_INTERVAL);
    LOGGER.info("metric persist interval: {}", persistIntervalStr);
    Preconditions.checkState(StringUtils.isNotEmpty(persistIntervalStr),
        METRIC_PERSIST_INTERVAL + " must be specified");

    persistInterval = Long.valueOf(conf.get(METRIC_PERSIST_INTERVAL));
    Preconditions.checkState(persistInterval > 0,
        METRIC_PERSIST_INTERVAL + " must be greater than zero");

    String persistName = conf.get(METRIC_PERSIST_CLASS);
    LOGGER.info("metric persist class: {}", persistName);
    Preconditions.checkState(StringUtils.isNotEmpty(persistName),
        METRIC_PERSIST_CLASS + " must be specified");

    persister = (Persist) Class.forName(persistName).newInstance();
    persister.setConf(conf);

    saver = new Saver();
  }

  @Override
  public void start() {
    LOGGER.info("metric starting...");

    LOGGER.info("metric.persister starting...");
    persister.start();
    LOGGER.info("metric.persister started");

    LOGGER.info("metric saver starting...");
    saver.start();
    LOGGER.info("metric saver started");

    LOGGER.info("metric started");
  }

  @Override
  public void stop() {
    LOGGER.info("metric stopping...");

    LOGGER.info("metric.saver stopping");
    saver.interrupt();
    try {
      saver.join();
    } catch (InterruptedException e) {
      LOGGER.warn("metric saver await for termination, but interrupted");
    }
    LOGGER.info("metric.saver stopped");

    LOGGER.info("metric.persister stopping...");
    persister.stop();
    LOGGER.info("metric.perssiter stopped");

    LOGGER.info("metric stopped");
  }

  public void increment(String name, String topic, long delta) {
    String counterName = name + Constants.COLON + topic;

    lock.readLock().lock();

    if (!metrics.containsKey(counterName)) {
      lock.readLock().unlock();

      lock.writeLock().lock();

      try {
        if (!metrics.containsKey(counterName)) {
          metrics.put(counterName, new AtomicLong(0L));
        }

        lock.readLock().lock();
      } finally {
        lock.writeLock().unlock();
      }
    }

    try {
      metrics.get(counterName).addAndGet(delta);
    } finally {
      lock.readLock().unlock();
    }
  }

  public interface Persist extends Configurable, Lifecycle {

    void persist(List<Counter> counters);

  }

  public static class Counter {

    private String name;
    private String topic;
    private long delta;

    public Counter() {

    }

    public Counter(String name, String topic, long delta) {
      this.name = name;
      this.topic = topic;
      this.delta = delta;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }

    public long getDelta() {
      return delta;
    }

    public void setDelta(long delta) {
      this.delta = delta;
    }

    @Override
    public String toString() {
      return "Counter{" + "name='" + name + '\'' + ", topic='" + topic + '\'' + ", delta=" + delta + '}';
    }

  }

  private class Saver extends Thread {

    private void persist(Map<String, AtomicLong> metrics) {
      if (MapUtils.isEmpty(metrics)) {
        return;
      }

      lock.writeLock().lock();

      List<Counter> counters = new ArrayList<>();

      for (Map.Entry<String, AtomicLong> entry : metrics.entrySet()) {
        String[] keys = entry.getKey().split(Constants.COLON);
        long counter = entry.getValue().get();

        String name = keys[0];
        String topic = keys[1];

        counters.add(new Counter(name, topic, counter));
      }

      metrics.clear();

      lock.writeLock().unlock();

      try {
        persister.persist(counters);
      } catch (Exception e) {
        LOGGER.error("metric persist error: " + ExceptionUtils.getFullStackTrace(e));
      }
    }

    @Override
    public void run() {
      while (!isInterrupted()) {
        try {
          Thread.sleep(persistInterval);
        } catch (InterruptedException e) {
          LOGGER.info("metric saver persist sleep, but interrupted");

          break;
        }

        persist(metrics);
      }

      persist(metrics);
    }

  }

}
