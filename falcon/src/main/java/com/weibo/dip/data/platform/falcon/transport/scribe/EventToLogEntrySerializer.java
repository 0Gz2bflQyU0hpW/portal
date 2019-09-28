package com.weibo.dip.data.platform.falcon.transport.scribe;

import java.nio.ByteBuffer;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;

public class EventToLogEntrySerializer implements FlumeEventSerializer {

  private String scribeCategoryHeaderKey;

  @Override
  public void configure(Context context) {
    scribeCategoryHeaderKey =
        context.getString(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_CATEGORY_HEADER);

    if (scribeCategoryHeaderKey == null) {
      throw new RuntimeException(
          ScribeSinkConfigurationConstants.CONFIG_SCRIBE_CATEGORY_HEADER + " is not configured.");
    }
  }

  @Override
  public void configure(ComponentConfiguration componentConfiguration) {}

  @Override
  public LogEntry serialize(Event event) {
    LogEntry entry = new LogEntry();

    entry.setMessage(ByteBuffer.wrap(event.getBody()));

    String category = event.getHeaders().get(scribeCategoryHeaderKey);
    if (category == null) {
      category = "default";
    }

    entry.setCategory(category);

    return entry;
  }

  @Override
  public void close() {}
}
