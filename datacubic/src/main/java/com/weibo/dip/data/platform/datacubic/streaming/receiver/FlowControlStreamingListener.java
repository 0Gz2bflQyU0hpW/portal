package com.weibo.dip.data.platform.datacubic.streaming.receiver;

import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 16/12/27.
 */
public class FlowControlStreamingListener extends StreamingListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlowControlStreamingListener.class);

    @Override
    public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
        LOGGER.info("BatchStared: " + batchStarted.batchInfo().batchTime().milliseconds());
    }

    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
        LOGGER.info("BatchCompleted: " + batchCompleted.batchInfo().batchTime().milliseconds());
    }

}
