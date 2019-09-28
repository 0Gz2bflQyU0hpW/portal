package com.weibo.dip.streaming.bean.application;

/**
 * Application attempt.
 *
 * @author yurun
 */
public class ApplicationAttempt {
  private String startTime;
  private String endTime;
  private String lastUpdated;
  private int duration;
  private String sparkUser;
  private boolean completed;
  private long endTimeEpoch;
  private long lastUpdatedEpoch;
  private long startTimeEpoch;

  public ApplicationAttempt() {}

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public String getEndTime() {
    return endTime;
  }

  public void setEndTime(String endTime) {
    this.endTime = endTime;
  }

  public String getLastUpdated() {
    return lastUpdated;
  }

  public void setLastUpdated(String lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  public int getDuration() {
    return duration;
  }

  public void setDuration(int duration) {
    this.duration = duration;
  }

  public String getSparkUser() {
    return sparkUser;
  }

  public void setSparkUser(String sparkUser) {
    this.sparkUser = sparkUser;
  }

  public boolean isCompleted() {
    return completed;
  }

  public void setCompleted(boolean completed) {
    this.completed = completed;
  }

  public long getEndTimeEpoch() {
    return endTimeEpoch;
  }

  public void setEndTimeEpoch(long endTimeEpoch) {
    this.endTimeEpoch = endTimeEpoch;
  }

  public long getLastUpdatedEpoch() {
    return lastUpdatedEpoch;
  }

  public void setLastUpdatedEpoch(long lastUpdatedEpoch) {
    this.lastUpdatedEpoch = lastUpdatedEpoch;
  }

  public long getStartTimeEpoch() {
    return startTimeEpoch;
  }

  public void setStartTimeEpoch(long startTimeEpoch) {
    this.startTimeEpoch = startTimeEpoch;
  }

  @Override
  public String toString() {
    return "ApplicationAttempt{"
        + "startTime='"
        + startTime
        + '\''
        + ", endTime='"
        + endTime
        + '\''
        + ", lastUpdated='"
        + lastUpdated
        + '\''
        + ", duration="
        + duration
        + ", sparkUser='"
        + sparkUser
        + '\''
        + ", completed="
        + completed
        + ", endTimeEpoch="
        + endTimeEpoch
        + ", lastUpdatedEpoch="
        + lastUpdatedEpoch
        + ", startTimeEpoch="
        + startTimeEpoch
        + '}';
  }
}
