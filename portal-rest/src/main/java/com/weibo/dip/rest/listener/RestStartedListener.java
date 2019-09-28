package com.weibo.dip.rest.listener;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.commons.GlobalResourceManager;
import com.weibo.dip.data.platform.kafka.KafkaWriter;
import com.weibo.dip.rest.Conf;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.Environment;

/**
 * Rest started listener.
 *
 * @author yurun
 */
public class RestStartedListener implements ApplicationListener<ApplicationPreparedEvent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestStartedListener.class);

  @Override
  public void onApplicationEvent(ApplicationPreparedEvent event) {
    Environment env = event.getApplicationContext().getEnvironment();

    String kafkaServers = env.getProperty(Conf.KAFKA_SERVERS);
    Preconditions.checkState(
        StringUtils.isNotEmpty(kafkaServers), Conf.KAFKA_SERVERS + " must be specified");

    LOGGER.info(Conf.KAFKA_SERVERS + " : {}", kafkaServers);

    KafkaWriter kafkaWriter = new KafkaWriter(kafkaServers);

    GlobalResourceManager.register(kafkaWriter);

    LOGGER.info("rest service started");
  }
}
