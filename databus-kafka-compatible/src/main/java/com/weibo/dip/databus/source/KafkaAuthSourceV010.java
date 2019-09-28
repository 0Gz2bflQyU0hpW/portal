package com.weibo.dip.databus.source;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Source;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jianhong1 on 2018/6/27.
 */
public class KafkaAuthSourceV010 extends Source {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAuthSourceV010.class);

  private static final String BOOTSTRAP_SERVERS = "source.kafka.bootstrap.servers";
  private static final String GROUP_ID = "source.kafka.group.id";
  private static final String AUTO_OFFSET_RESET = "source.kafka.auto.offset.reset";
  private static final String KEY_DESERIALIZER = "source.kafka.key.deserializer";
  private static final String VALUE_DESERIALIZER = "source.kafka.value.deserializer";

  private static final String SASL_MECHANISM = "source.kafka.sasl.mechanism";
  private static final String SECURITY_PROTOCOL = "source.kafka.security.protocol";
  private static final String SASL_JAAS_CONFIG = "source.kafka.sasl.jaas.config";

  private static final String TOPIC_AND_THREADS = "source.kafka.topic.and.threads";
  private static final long STOP_SLEEP = 3000L;
  private static final long POLL_TIMEOUT = 1000L;

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private ExecutorService threadPool = Executors.newCachedThreadPool();

  private String bootstrapServers;
  private String groupId;
  private String topicAndThreads;
  private String autoOffsetReset;
  private String keyDeserializer;
  private String valueDeserializer;
  private String saslMechanism;
  private String securityProtocol;
  private String saslJaasConfig;

  @Override
  public void setConf(Configuration conf) {
    name =
        conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    bootstrapServers = conf.get(BOOTSTRAP_SERVERS);
    Preconditions.checkState(StringUtils.isNotEmpty(bootstrapServers),
        name + " " + BOOTSTRAP_SERVERS + " must be specified");

    groupId = conf.get(GROUP_ID);
    Preconditions.checkState(StringUtils.isNotEmpty(groupId),
        name + " " + GROUP_ID + " must be specified");

    topicAndThreads = conf.get(TOPIC_AND_THREADS);
    Preconditions.checkState(StringUtils.isNotEmpty(topicAndThreads),
        name + " " + TOPIC_AND_THREADS + " must be specified");

    keyDeserializer = conf.get(KEY_DESERIALIZER);
    Preconditions.checkState(StringUtils.isNotEmpty(keyDeserializer),
        name + " " + KEY_DESERIALIZER + " must be specified");

    valueDeserializer = conf.get(VALUE_DESERIALIZER);
    Preconditions.checkState(StringUtils.isNotEmpty(valueDeserializer),
        name + " " + VALUE_DESERIALIZER + " must be specified");

    saslMechanism = conf.get(SASL_MECHANISM);
    Preconditions.checkState(StringUtils.isNotEmpty(saslMechanism),
        name + " " + saslMechanism + " must be specified");

    securityProtocol = conf.get(SECURITY_PROTOCOL);
    Preconditions.checkState(StringUtils.isNotEmpty(securityProtocol),
        name + " " + SECURITY_PROTOCOL + " must be specified");

    saslJaasConfig = conf.get(SASL_JAAS_CONFIG);
    Preconditions.checkState(StringUtils.isNotEmpty(saslJaasConfig),
        name + " " + SASL_JAAS_CONFIG + " must be specified");

    autoOffsetReset = conf.get(AUTO_OFFSET_RESET);
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    Properties properties = new Properties();

    properties.put("bootstrap.servers", bootstrapServers);
    properties.put("group.id", groupId);
    properties.put("key.deserializer", keyDeserializer);
    properties.put("value.deserializer", valueDeserializer);

    properties.put("sasl.mechanism", saslMechanism);
    properties.put("security.protocol", securityProtocol);
    properties.put("sasl.jaas.config", saslJaasConfig);

    if (Strings.isNotEmpty(autoOffsetReset)) {
      properties.put("auto.offset.reset", autoOffsetReset);
    }

    Map<String, Integer> topicThreads = new HashMap<>();
    for (String topicAndThread : topicAndThreads.split(Constants.COMMA)) {
      String[] strs = topicAndThread.split(Constants.COLON);

      if (ArrayUtils.isNotEmpty(strs) && strs.length == 2) {
        String topic = strs[0];
        int threadNumber = Integer.parseInt(strs[1]);

        topicThreads.put(topic, threadNumber);
      } else {
        throw new IllegalArgumentException(name + " " + topicAndThreads
            + " wrong format, the format should be 'topic1:number1,topic2:number2'");
      }
    }

    for (Entry<String, Integer> topicThread : topicThreads.entrySet()) {
      String topic = topicThread.getKey();
      Integer thread = topicThread.getValue();

      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

      for (int i = 0; i < thread; i++) {
        threadPool.execute(new KafkaConsumerRunner(consumer, topic));
      }
    }

    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    LOGGER.info("{} stopping...", name);

    closed.set(true);

    threadPool.shutdown();

    try {
      while (!threadPool.awaitTermination(STOP_SLEEP, TimeUnit.MILLISECONDS)) {
        LOGGER.info("{} threadPool await termination", name);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} threadPool await termination, but interrupted", name);
    }

    LOGGER.info("{} stopped", name);
  }

  private class KafkaConsumerRunner implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    private KafkaConsumerRunner(KafkaConsumer<String, String> consumer, String topic) {
      this.consumer = consumer;
      this.topic = topic;
    }

    @Override
    public void run() {
      LOGGER.info(name + " consumer " + Thread.currentThread().getName() + " started");

      try {
        consumer.subscribe(Collections.singletonList(topic));
        while (!closed.get()) {
          ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
          for (ConsumerRecord<String, String> record : records) {
            deliver(new Message(topic, record.value()));
          }
        }
      } catch (WakeupException e) {
        if (!closed.get()) {
          throw e;
        }
      } finally {
        consumer.close();
      }

      LOGGER.info(name + " consumer " + Thread.currentThread().getName() + " stopped");
    }
  }
}
