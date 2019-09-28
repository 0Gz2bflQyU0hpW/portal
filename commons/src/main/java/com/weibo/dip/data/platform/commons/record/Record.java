package com.weibo.dip.data.platform.commons.record;

import java.util.HashMap;
import java.util.Map;

/** Created by yurun on 18/1/22. */
public class Record {
  private String business;
  private long timestamp;
  private Map<String, String> dimensions;
  private Map<String, Long> metrics;

  public Record() {
    dimensions = new HashMap<>();
    metrics = new HashMap<>();
  }

  public String getBusiness() {
    return business;
  }

  public void setBusiness(String business) {
    this.business = business;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Map<String, String> getDimensions() {
    return dimensions;
  }

  public void addDimension(String name, String value) {
    dimensions.put(name, value);
  }

  public Map<String, Long> getMetrics() {
    return metrics;
  }

  public void addMetric(String name, long value) {
    metrics.put(name, value);
  }

  @Override
  public String toString() {
    return "Record{"
        + "business='"
        + business
        + '\''
        + ", timestamp="
        + timestamp
        + ", dimensions="
        + dimensions
        + ", metrics="
        + metrics
        + '}';
  }
}
