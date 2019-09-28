package com.weibo.dip.data.platform.datacubic.streaming.receiver;

import org.apache.spark.streaming.scheduler.*;

/**
 * Created by yurun on 16/12/27.
 */
public class StreamingListener implements org.apache.spark.streaming.scheduler.StreamingListener {

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
    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {
    }

    @Override
    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {
    }

}
