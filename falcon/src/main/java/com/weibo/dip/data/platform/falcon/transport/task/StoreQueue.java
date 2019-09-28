package com.weibo.dip.data.platform.falcon.transport.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Wen on 2017/2/20.
 */
public class StoreQueue {

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreQueue.class);

    private static final StoreQueue STORE_QUEUE = new StoreQueue();

    private BlockingQueue<String> queue = new LinkedBlockingQueue<>();

    private StoreQueue() {

    }

    public static StoreQueue getInstance() {
        return STORE_QUEUE;
    }

    public void offer(String filePath) {
        queue.offer(filePath);
    }

    public void offer(List<String> fileList) {
        queue.addAll(fileList);
    }

    public String take() throws InterruptedException {
        return queue.take();
    }

    public String poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return queue.poll(timeout,timeUnit);
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }


}
