package com.weibo.dip.streaming.bean;

/**
 * Statistic.
 *
 * @author yurun
 */
public class Statistic {
  private String startTime;
  private int batchDuration;
  private int numReceivers;
  private int numActiveReceivers;
  private int numInactiveReceivers;
  private int numTotalCompletedBatches;
  private int numRetainedCompletedBatches;
  private int numActiveBatches;
  private int numProcessedRecords;
  private int numReceivedRecords;
  private double avgInputRate;
  private int avgSchedulingDelay;
  private int avgProcessingTime;
  private int avgTotalDelay;

  public Statistic() {}

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public int getBatchDuration() {
    return batchDuration;
  }

  public void setBatchDuration(int batchDuration) {
    this.batchDuration = batchDuration;
  }

  public int getNumReceivers() {
    return numReceivers;
  }

  public void setNumReceivers(int numReceivers) {
    this.numReceivers = numReceivers;
  }

  public int getNumActiveReceivers() {
    return numActiveReceivers;
  }

  public void setNumActiveReceivers(int numActiveReceivers) {
    this.numActiveReceivers = numActiveReceivers;
  }

  public int getNumInactiveReceivers() {
    return numInactiveReceivers;
  }

  public void setNumInactiveReceivers(int numInactiveReceivers) {
    this.numInactiveReceivers = numInactiveReceivers;
  }

  public int getNumTotalCompletedBatches() {
    return numTotalCompletedBatches;
  }

  public void setNumTotalCompletedBatches(int numTotalCompletedBatches) {
    this.numTotalCompletedBatches = numTotalCompletedBatches;
  }

  public int getNumRetainedCompletedBatches() {
    return numRetainedCompletedBatches;
  }

  public void setNumRetainedCompletedBatches(int numRetainedCompletedBatches) {
    this.numRetainedCompletedBatches = numRetainedCompletedBatches;
  }

  public int getNumActiveBatches() {
    return numActiveBatches;
  }

  public void setNumActiveBatches(int numActiveBatches) {
    this.numActiveBatches = numActiveBatches;
  }

  public int getNumProcessedRecords() {
    return numProcessedRecords;
  }

  public void setNumProcessedRecords(int numProcessedRecords) {
    this.numProcessedRecords = numProcessedRecords;
  }

  public int getNumReceivedRecords() {
    return numReceivedRecords;
  }

  public void setNumReceivedRecords(int numReceivedRecords) {
    this.numReceivedRecords = numReceivedRecords;
  }

  public double getAvgInputRate() {
    return avgInputRate;
  }

  public void setAvgInputRate(double avgInputRate) {
    this.avgInputRate = avgInputRate;
  }

  public int getAvgSchedulingDelay() {
    return avgSchedulingDelay;
  }

  public void setAvgSchedulingDelay(int avgSchedulingDelay) {
    this.avgSchedulingDelay = avgSchedulingDelay;
  }

  public int getAvgProcessingTime() {
    return avgProcessingTime;
  }

  public void setAvgProcessingTime(int avgProcessingTime) {
    this.avgProcessingTime = avgProcessingTime;
  }

  public int getAvgTotalDelay() {
    return avgTotalDelay;
  }

  public void setAvgTotalDelay(int avgTotalDelay) {
    this.avgTotalDelay = avgTotalDelay;
  }

  @Override
  public String toString() {
    return "Statistic{"
        + "startTime='"
        + startTime
        + '\''
        + ", batchDuration="
        + batchDuration
        + ", numReceivers="
        + numReceivers
        + ", numActiveReceivers="
        + numActiveReceivers
        + ", numInactiveReceivers="
        + numInactiveReceivers
        + ", numTotalCompletedBatches="
        + numTotalCompletedBatches
        + ", numRetainedCompletedBatches="
        + numRetainedCompletedBatches
        + ", numActiveBatches="
        + numActiveBatches
        + ", numProcessedRecords="
        + numProcessedRecords
        + ", numReceivedRecords="
        + numReceivedRecords
        + ", avgInputRate="
        + avgInputRate
        + ", avgSchedulingDelay="
        + avgSchedulingDelay
        + ", avgProcessingTime="
        + avgProcessingTime
        + ", avgTotalDelay="
        + avgTotalDelay
        + '}';
  }
}
