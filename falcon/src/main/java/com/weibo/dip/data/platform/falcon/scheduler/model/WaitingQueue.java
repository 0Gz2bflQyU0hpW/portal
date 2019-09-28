package com.weibo.dip.data.platform.falcon.scheduler.model;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by Wen on 2017/2/16.
 *
 */
public class WaitingQueue {
    private ConcurrentHashMap<String,PriorityBlockingQueue<Task>> waitingMap = new ConcurrentHashMap<>();

    private final static Logger LOGGER = LoggerFactory.getLogger(WaitingQueue.class);

    private WaitingQueue() {

    }

    public void prepare(String[] jobTypes) {
        if (!ArrayUtils.isEmpty(jobTypes)) {
            for (String jobType : jobTypes) {
                if (CollectionUtils.isEmpty(waitingMap.get(jobType))) {
                    waitingMap.put(jobType,new PriorityBlockingQueue<>());
                }
            }
        }
        else {
            LOGGER.error("jobTypes is empty or null");
        }
    }

    public void offer(Task task) {
        String jobType = task.getJobType();
        if (waitingMap.get(jobType) == null) {
            LOGGER.error("We don't have this kind of job ");
        }
        else{
            PriorityBlockingQueue<Task> queue = waitingMap.get(jobType);
            queue.offer(task);
            LOGGER.info("Add " + task.getJobName() + "successfully and the jobType is " + task.getJobType());
        }
    }

    public Task poll(String jobType) {
        if (waitingMap.get(jobType) == null) {
            LOGGER.error("The jobType: " + jobType + "doesn't has any job in queue" );
            return null;
        }
        else {
//            return waitingMap.get(jobType).poll();
            try {
                return waitingMap.get(jobType).take();
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public int size() {
        int size = 0;
        for (String jobType :waitingMap.keySet()) {
            size += waitingMap.get(jobType).size();
        }
        return size;
    }

    public int size(String jobType) {
        if (CollectionUtils.isEmpty(waitingMap.get(jobType))) {
            LOGGER.error("The jobType: " + jobType + "doesn't has any job in queue" );
            return 0;
        }
        else {
            return waitingMap.get(jobType).size();
        }
    }

    public static class WaitingQueueUtil {
        private static final WaitingQueue WAITING_QUEUE= new WaitingQueue();
        public static WaitingQueue getInstance(){
            return WAITING_QUEUE;
        }
    }

    @Override
    public String toString() {
        StringBuffer content = new StringBuffer();
        for (String jobType : waitingMap.keySet()) {
            content.append(jobType + " : " + waitingMap.get(jobType));
        }
        return content.toString();
    }
}

