package com.weibo.dip.data.platform.datacubic.streaming.receiver;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yurun on 16/12/27.
 */
public class FlowControlReceiver extends Receiver<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlowControlReceiver.class);

    private ExecutorService executor = null;

    public FlowControlReceiver() {
        super(StorageLevel.MEMORY_ONLY());
    }

    @Override
    public void onStart() {
        LOGGER.info("FlowControlReceiver starting ...");

        executor = Executors.newSingleThreadExecutor();

        executor.submit(() -> {
            while (!isStopped()) {
                store(String.valueOf(System.currentTimeMillis()));

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.warn("store sleep interrupted");
                }
            }
        });

        LOGGER.info("FloControlReceiver started");
    }

    @Override
    public void onStop() {
        LOGGER.info("FlowControlReceiver stoping ...");

        executor.shutdown();

        while (!executor.isTerminated()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.warn("FlowControlReceiver onStop interrupted");
            }
        }

        LOGGER.info("FlowControlReceiver stoped");
    }

}
