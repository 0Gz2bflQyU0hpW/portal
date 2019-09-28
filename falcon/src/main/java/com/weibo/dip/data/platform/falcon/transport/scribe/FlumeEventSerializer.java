package com.weibo.dip.data.platform.falcon.transport.scribe;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;

public interface FlumeEventSerializer extends Configurable, ConfigurableComponent {
  LogEntry serialize(Event event);

  void close();
}
