package com.weibo.dip.databus.sink;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 17/8/31.
 */
public class KafkaSink extends Sink {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

  private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

  private static final String KEY_SERIALIZER = "key.serializer";
  private static final String VALUE_SERIALIZER = "value.serializer";

  private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

  private String brokers;

  private Producer<String, String> producer;

  @Override
  public void setConf(Configuration conf) throws Exception {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + KafkaSink.class.getSimpleName();

    brokers = conf.get(BOOTSTRAP_SERVERS);
    Preconditions.checkState(StringUtils.isNotEmpty(brokers),
        name + " " + BOOTSTRAP_SERVERS + " must be specified");
  }

  @Override
  public void start() {
    Map<String, Object> config = new HashMap<>();

    config.put(BOOTSTRAP_SERVERS, brokers);

    config.put(KEY_SERIALIZER, STRING_SERIALIZER);

    config.put(VALUE_SERIALIZER, STRING_SERIALIZER);

    producer = new KafkaProducer<>(config);

    LOGGER.info(name + " started");
  }

  @Override
  public void process(Message message) throws Exception {
    producer.send(new ProducerRecord<>(message.getTopic(), message.getData()));
  }

  @Override
  public void stop() {
    producer.close();

    LOGGER.info(name + " stoped");
  }

}
