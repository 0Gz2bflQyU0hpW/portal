package com.weibo.dip.data.platform.datacubic.business.summonagg.service.impl;

import com.google.common.util.concurrent.RateLimiter;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.business.summonagg.service.KafkaService;
import com.weibo.dip.data.platform.datacubic.business.util.DataProperties;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

/**
 * Created by liyang28 on 2018/7/28.
 */
public class KafkaServiceImpl implements KafkaService, Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceImpl.class);
  private static final String RESULT_TOPIC = "kafka.topic.send";
  private static final String PRODUCER_CONFIG = "producerConfig";
  private static final String SUMMON_DAILY_AGG_CONFIG = "summondailyagg.properties";
  private Map<String, Object> producerConfig;
  private String kafkaTopicSend;


  public KafkaServiceImpl() {
  }

  @Override
  public void setConfig() throws IOException {
    Map<String, String> configMsg = DataProperties.loads(SUMMON_DAILY_AGG_CONFIG);
    producerConfig = GsonUtil.fromJson(configMsg.get(PRODUCER_CONFIG),
        GsonUtil.GsonType.OBJECT_MAP_TYPE);
    kafkaTopicSend = configMsg.get(RESULT_TOPIC);
  }

  @Override
  public void sendKafka(JavaRDD<String> lineRdd, LongAccumulator matchCount) throws IOException {
    setConfig();
    //发送kafka至summon平台
    lineRdd.foreachPartition((Iterator<String> it) -> {
      Producer<String, String> producer = null;
      try {
        producer = new KafkaProducer<>(producerConfig);
        RateLimiter limiter = RateLimiter.create(100000.0); //kafka限流 byte/s
        while (it.hasNext()) {
          String next = it.next();
          limiter.acquire(next.getBytes().length);
          try {
            matchCount.add(1L);
            producer.send(new ProducerRecord<>(kafkaTopicSend, next));
          } catch (Exception e) {
            LOGGER.error("producer send record error: {} {}", next, ExceptionUtils
                .getFullStackTrace(e));
          }
        }

      } catch (Exception e) {
        LOGGER.error("producer send error: {}", ExceptionUtils.getFullStackTrace(e));
      } finally {
        if (producer != null) {
          producer.close();
        }
      }
    });
  }
}
