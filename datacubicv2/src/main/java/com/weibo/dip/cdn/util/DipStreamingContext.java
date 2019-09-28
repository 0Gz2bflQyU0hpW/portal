package com.weibo.dip.cdn.util;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by yurun on 17/11/30.
 */
public class DipStreamingContext extends JavaStreamingContext {

    public DipStreamingContext(SparkConf conf, Duration batchDuration) {
        super(conf, batchDuration);
    }

    @Override
    public void start() {
        DipStreaming.start(ssc());
    }

    @Override
    public boolean awaitTerminationOrTimeout(long timeout) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop(boolean stopSparkContext, boolean stopGracefully) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop(boolean stopSparkContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException();
    }

}
