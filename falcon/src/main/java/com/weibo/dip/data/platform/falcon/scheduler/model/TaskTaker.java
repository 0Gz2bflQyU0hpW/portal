package com.weibo.dip.data.platform.falcon.scheduler.model;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Wen on 2017/2/16.
 *
 */
public class TaskTaker implements Runnable {

    private long strategy;

    private String jobType;

    private int takeNum;

    private static  final Logger LOGGER = LoggerFactory.getLogger(TaskTaker.class);

    private RunningList runningList = RunningList.RunningListUtil.getInstance();

    private WaitingQueue waitingQueue = WaitingQueue.WaitingQueueUtil.getInstance();

    private WorkerPool workerPool = WorkerPool.WorkerPoolUtil.getInstance();

    public TaskTaker(long strategy, String jobType, int takeNum) {
        this.strategy = strategy;
        this.jobType = jobType;
        this.takeNum = takeNum;
    }

    public void executeTask() {
        //noinspection InfiniteLoopStatement
        while (true) {
            for (int i=0;i<takeNum;i++) {
                workerPool.execute(waitingQueue.poll(jobType));
            }
            try {
                Thread.sleep(strategy);
            } catch (InterruptedException e) {
                LOGGER.error(ExceptionUtils.getFullStackTrace(e));
            }
        }
    }

    public long getStrategy() {
        return strategy;
    }

    public void setStrategy(long strategy) {
        this.strategy = strategy;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public int getTakeNum() {
        return takeNum;
    }

    public void setTakeNum(int takeNum) {
        this.takeNum = takeNum;
    }

    @Override
    public void run() {
        executeTask();
    }
}
