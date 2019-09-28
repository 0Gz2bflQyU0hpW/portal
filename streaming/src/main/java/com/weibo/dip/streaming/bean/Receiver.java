package com.weibo.dip.streaming.bean;

import java.util.List;

/** @author yurun */
public class Receiver {
  private int streamId;
  private String streamName;
  private boolean active;
  private String executorId;
  private String executorHost;
  private double avgEventRate;
  private List<EventRate> eventRates;

  public Receiver() {}

  public int getStreamId() {
    return streamId;
  }

  public void setStreamId(int streamId) {
    this.streamId = streamId;
  }

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public String getExecutorId() {
    return executorId;
  }

  public void setExecutorId(String executorId) {
    this.executorId = executorId;
  }

  public String getExecutorHost() {
    return executorHost;
  }

  public void setExecutorHost(String executorHost) {
    this.executorHost = executorHost;
  }

  public double getAvgEventRate() {
    return avgEventRate;
  }

  public void setAvgEventRate(double avgEventRate) {
    this.avgEventRate = avgEventRate;
  }

  public List<EventRate> getEventRates() {
    return eventRates;
  }

  public void setEventRates(List<EventRate> eventRates) {
    this.eventRates = eventRates;
  }

  @Override
  public String toString() {
    return "Receiver{"
        + "streamId="
        + streamId
        + ", streamName='"
        + streamName
        + '\''
        + ", active="
        + active
        + ", executorId='"
        + executorId
        + '\''
        + ", executorHost='"
        + executorHost
        + '\''
        + ", avgEventRate="
        + avgEventRate
        + ", eventRates="
        + eventRates
        + '}';
  }
}
