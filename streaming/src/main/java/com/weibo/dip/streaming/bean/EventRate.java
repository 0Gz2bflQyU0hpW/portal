package com.weibo.dip.streaming.bean;

/**
 * EventRate.
 *
 * @author yurun
 */
public class EventRate {
  private long timestamp;
  private double rate;

  public EventRate() {}

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public double getRate() {
    return rate;
  }

  public void setRate(double rate) {
    this.rate = rate;
  }

  @Override
  public String toString() {
    return "EventRate{" + "timestamp=" + timestamp + ", rate=" + rate + '}';
  }
}
