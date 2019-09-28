package com.weibo.dip.databus.source;

import cn.sina.api.commons.cache.MemcacheClient;
import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Source;
import com.weibo.trigger.common.bean.MessageBatch;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by jianhong1 on 2018/7/31.
 */
public class TriggerSource extends Source {
  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerSource.class);

  private static final String BEAN_NAME = "source.trigger.bean.name";
  private static final String TOPIC = "source.trigger.topic";
  private static final String THREADS_NUMBER = "source.threads.number";
  private static final long AWAIT_TERMINATION_TIME = 30;
  private static final long THREAD_SLEEP_TIME_MILLISECOND = 60000;
  private boolean flag = true;

  private String beanName;
  private String topic;
  private String threadsNumber;

  private ClassPathXmlApplicationContext applicationContext;
  private ExecutorService executorService;

  @Override
  public void setConf(Configuration conf) throws Exception {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    beanName = conf.get(BEAN_NAME);
    Preconditions.checkNotNull(beanName, name + " " + BEAN_NAME + " must be specified");
    LOGGER.info("Property: " + BEAN_NAME + "=" + beanName);

    topic = conf.get(TOPIC);
    Preconditions.checkNotNull(topic, name + " " + TOPIC + " must be specified");
    LOGGER.info("Property: " + TOPIC + "=" + topic);

    threadsNumber = conf.get(THREADS_NUMBER);
    Preconditions.checkState(StringUtils.isNumeric(threadsNumber),
        name + " " + THREADS_NUMBER + " must be numeric");
    LOGGER.info("Property: " + THREADS_NUMBER + "=" + threadsNumber);
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    applicationContext = new ClassPathXmlApplicationContext("classpath:trigger-client-video.xml");

    int numThreads = Integer.parseInt(threadsNumber);
    executorService = Executors.newFixedThreadPool(numThreads);
    for (int index = 0; index < numThreads; index++) {
      executorService.execute(new Streamer());
    }

    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    LOGGER.info("{} stopping...", name);

    flag = false;

    executorService.shutdown();
    try {
      while (!executorService.awaitTermination(AWAIT_TERMINATION_TIME, TimeUnit.SECONDS)) {
        LOGGER.info("{} executor await termination", name);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} executor await termination, but interrupt", name);
    }

    if (applicationContext != null) {
      applicationContext.close();
      LOGGER.info("applicationContext stopped");
    }
    LOGGER.info("{} stopped", name);
  }

  private class Streamer implements Runnable {
    private MemcacheClient memcacheClient;

    @Override
    public void run() {
      String threadName = Thread.currentThread().getName();
      LOGGER.info("{} {} started", name, threadName);

      memcacheClient = (MemcacheClient) applicationContext.getBean(beanName);

      while (flag) {
        MessageBatch batch = (MessageBatch) memcacheClient.get(topic);

        if (batch != null) {
          List<String> jsonList = batch.getResultJson();
          for (String json: jsonList) {
            deliver(new Message(topic, json));
          }
        } else {
          try {
            LOGGER.info("{} {} is empty, {} sleeping", beanName, topic, threadName);
            Thread.sleep(THREAD_SLEEP_TIME_MILLISECOND);
          } catch (InterruptedException e) {
            LOGGER.warn("thread:{} is sleeping, but interrupted", threadName);
          }
        }
      }

      LOGGER.info("{} {} stopped", name, threadName);
    }
  }
}
