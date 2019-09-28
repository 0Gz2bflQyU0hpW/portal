package com.weibo.dip.databus.sink;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jianhong1 on 2018/9/25.
 */
public class KafkaSinkV010 extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSinkV010.class);

  private static final String BROKERS = "sink.kafka.bootstrap.servers";
  private static final String KEY_SERIALIZER = "sink.kafka.key.serializer";
  private static final String VALUE_SERIALIZER = "sink.kafka.value.serializer";
  private static final String ACKS = "sink.kafka.acks";
  private static final String COMPRESSION = "sink.kafka.compression.type";

  private String brokers;
  private String keySerializer;
  private String valueSerializer;
  private String acks;
  private String compression;

  private KafkaProducer<String, String> producer;

  @Override
  public void process(Message message) throws Exception {
    producer.send(new ProducerRecord<>(message.getTopic(), message.getData()));
  }

  @Override
  public void setConf(Configuration conf) throws Exception {
    name =
        conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + KafkaSinkV08.class.getSimpleName();

    brokers = conf.get(BROKERS);
    Preconditions.checkState(StringUtils.isNotEmpty(brokers),
        name + " " + BROKERS + " must be specified");
    LOGGER.info("properties: {}={}", BROKERS, brokers);

    keySerializer = conf.get(KEY_SERIALIZER);
    Preconditions.checkState(StringUtils.isNotEmpty(keySerializer),
        name + " " + KEY_SERIALIZER + " must be specified");
    LOGGER.info("properties: {}={}", KEY_SERIALIZER, keySerializer);

    valueSerializer = conf.get(VALUE_SERIALIZER);
    Preconditions.checkState(StringUtils.isNotEmpty(valueSerializer),
        name + " " + VALUE_SERIALIZER + " must be specified");
    LOGGER.info("properties: {}={}", VALUE_SERIALIZER, valueSerializer);

    acks = conf.get(ACKS);
    if (Strings.isNotEmpty(acks)) {
      LOGGER.info("properties: {}={}", ACKS, acks);
    }

    compression = conf.get(COMPRESSION);
    if (Strings.isNotEmpty(compression)) {
      LOGGER.info("properties: {}={}", COMPRESSION, compression);
    }
  }

  @Override
  public void start() {
    Properties props = new Properties();

    props.put("bootstrap.servers", brokers);
    props.put("key.serializer", keySerializer);
    props.put("value.serializer", valueSerializer);

    if (Strings.isNotEmpty(acks)) {
      props.put("acks", acks);
    }

    if (Strings.isNotEmpty(compression)) {
      props.put("compression.type", compression);
    }

    producer = new KafkaProducer<>(props);

    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    producer.close();

    LOGGER.info("{} stopped", name);
  }
}
