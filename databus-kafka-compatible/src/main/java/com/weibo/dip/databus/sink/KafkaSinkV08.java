package com.weibo.dip.databus.sink;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by jianhong1 on 18/6/27.
 */
public class KafkaSinkV08 extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSinkV08.class);

  private static final String BROKER_LIST = "sink.kafka.metadata.broker.list";
  private static final String SERIALIZER = "sink.kafka.serializer.class";
  private static final String ACKS = "sink.kafka.request.required.acks";

  private String brokers;
  private String acks;
  private String serializer;

  private Producer<String, String> producer;

  @Override
  public void setConf(Configuration conf) throws Exception {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + KafkaSinkV08.class.getSimpleName();

    brokers = conf.get(BROKER_LIST);
    Preconditions.checkState(StringUtils.isNotEmpty(brokers),
        name + " " + BROKER_LIST + " must be specified");

    serializer = conf.get(SERIALIZER);
    Preconditions.checkState(StringUtils.isNotEmpty(serializer),
        name + " " + SERIALIZER + " must be specified");

    acks = conf.get(ACKS);
  }

  @Override
  public void start() {
    Properties props = new Properties();

    props.put("metadata.broker.list", brokers);
    props.put("serializer.class", serializer);

    if (StringUtils.isNotEmpty(acks)) {
      props.put("request.required.acks", acks);
    }

    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<>(config);

    LOGGER.info(name + " started");
  }

  @Override
  public void process(Message message) throws Exception {
    producer.send(new KeyedMessage<>(message.getTopic(), message.getData()));
  }

  @Override
  public void stop() {
    producer.close();

    LOGGER.info(name + " stopped");
  }
}
