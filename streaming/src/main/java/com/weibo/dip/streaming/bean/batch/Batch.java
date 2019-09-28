package com.weibo.dip.streaming.bean.batch;

/**
 * Batch.
 *
 * @author yurun
 */
public class Batch {
  private int batchId;
  private String batchTime;
  private BatchStatus status;
  private int batchDuration;
  private int inputSize;
  private int schedulingDelay;
  private int processingTime;
  private int totalDelay;
  private int numActiveOutputOps;
  private int numCompletedOutputOps;
  private int numFailedOutputOps;
  private int numTotalOutputOps;

  public Batch() {}

  public int getBatchId() {
    return batchId;
  }

  public void setBatchId(int batchId) {
    this.batchId = batchId;
  }

  public String getBatchTime() {
    return batchTime;
  }

  public void setBatchTime(String batchTime) {
    this.batchTime = batchTime;
  }

  public BatchStatus getStatus() {
    return status;
  }

  public void setStatus(BatchStatus status) {
    this.status = status;
  }

  public int getBatchDuration() {
    return batchDuration;
  }

  public void setBatchDuration(int batchDuration) {
    this.batchDuration = batchDuration;
  }

  public int getInputSize() {
    return inputSize;
  }

  public void setInputSize(int inputSize) {
    this.inputSize = inputSize;
  }

  public int getSchedulingDelay() {
    return schedulingDelay;
  }

  public void setSchedulingDelay(int schedulingDelay) {
    this.schedulingDelay = schedulingDelay;
  }

  public int getProcessingTime() {
    return processingTime;
  }

  public void setProcessingTime(int processingTime) {
    this.processingTime = processingTime;
  }

  public int getTotalDelay() {
    return totalDelay;
  }

  public void setTotalDelay(int totalDelay) {
    this.totalDelay = totalDelay;
  }

  public int getNumActiveOutputOps() {
    return numActiveOutputOps;
  }

  public void setNumActiveOutputOps(int numActiveOutputOps) {
    this.numActiveOutputOps = numActiveOutputOps;
  }

  public int getNumCompletedOutputOps() {
    return numCompletedOutputOps;
  }

  public void setNumCompletedOutputOps(int numCompletedOutputOps) {
    this.numCompletedOutputOps = numCompletedOutputOps;
  }

  public int getNumFailedOutputOps() {
    return numFailedOutputOps;
  }

  public void setNumFailedOutputOps(int numFailedOutputOps) {
    this.numFailedOutputOps = numFailedOutputOps;
  }

  public int getNumTotalOutputOps() {
    return numTotalOutputOps;
  }

  public void setNumTotalOutputOps(int numTotalOutputOps) {
    this.numTotalOutputOps = numTotalOutputOps;
  }

  @Override
  public String toString() {
    return "Batch{"
        + "batchId="
        + batchId
        + ", batchTime='"
        + batchTime
        + '\''
        + ", status="
        + status
        + ", batchDuration="
        + batchDuration
        + ", inputSize="
        + inputSize
        + ", schedulingDelay="
        + schedulingDelay
        + ", processingTime="
        + processingTime
        + ", totalDelay="
        + totalDelay
        + ", numActiveOutputOps="
        + numActiveOutputOps
        + ", numCompletedOutputOps="
        + numCompletedOutputOps
        + ", numFailedOutputOps="
        + numFailedOutputOps
        + ", numTotalOutputOps="
        + numTotalOutputOps
        + '}';
  }
}
