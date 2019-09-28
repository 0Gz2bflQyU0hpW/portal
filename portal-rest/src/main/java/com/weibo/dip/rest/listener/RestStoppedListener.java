package com.weibo.dip.rest.listener;

import com.weibo.dip.data.platform.commons.GlobalResourceManager;
import java.io.IOException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;

/**
 * Rest stopped listener.
 *
 * @author yurun
 */
public class RestStoppedListener implements ApplicationListener<ContextClosedEvent> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RestStoppedListener.class);

  @Override
  public void onApplicationEvent(ContextClosedEvent event) {
    try {
      GlobalResourceManager.close();
    } catch (IOException e) {
      LOGGER.error("Global resource manager close error: {}", ExceptionUtils.getFullStackTrace(e));
    }

    LOGGER.info("rest service stoped");
  }
}
