package com.weibo.dip.data.platform.datacubic.streaming.monitor;

import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;

/**
 * Created by yurun on 17/3/2.
 */
public class StreamingMonitor implements StreamingListener {

  private String appName;

  public StreamingMonitor(String appName) {
    this.appName = appName;
  }

  @Override
  public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
  }

  @Override
  public void onReceiverError(StreamingListenerReceiverError receiverError) {
  }

  @Override
  public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
  }

  @Override
  public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
  }

  @Override
  public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
  }

  @Override
  public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
  }

  @Override
  public void onOutputOperationStarted(
      StreamingListenerOutputOperationStarted outputOperationStarted) {
  }

  @Override
  public void onOutputOperationCompleted(
      StreamingListenerOutputOperationCompleted outputOperationCompleted) {
  }

}
