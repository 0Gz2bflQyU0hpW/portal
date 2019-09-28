package com.weibo.dip.data.platform.falcon.scheduler.model;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Wen on 2017/2/16.
 *
 */
public class MainScheduler {

    private WaitingQueue waitingQueue = WaitingQueue.WaitingQueueUtil.getInstance();

    private RunningList runningList = RunningList.RunningListUtil.getInstance();

    private WorkerPool workerPool = WorkerPool.WorkerPoolUtil.getInstance();

    private static final Logger LOGGER = LoggerFactory.getLogger(MainScheduler.class);

    public void start(String[] jobTypes) {

//        String[] jobTypes = {"hello1","hello2"};
        waitingQueue.prepare(jobTypes);
        runningList.prepare(jobTypes);
        workerPool.prepare(jobTypes);

    }

    public void submitTask(Task task) {
        waitingQueue.offer(task);
    }
    //
    public void executeTask(String[] jobTypes) {
        if (ArrayUtils.isEmpty(jobTypes)) {
            LOGGER.error("The JobTypes is empty or null.Prepare failed!");
        }
        else {
            ExecutorService pool = Executors.newFixedThreadPool(jobTypes.length);
            for (String jobType : jobTypes) {
                TaskTaker taskTaker = new TaskTaker(1000,jobType,10); //only test
                pool.execute(taskTaker);
            }
        }

    }

    public void showStatus() {
        LOGGER.info("now the WaittingQueue : " + waitingQueue.size() + " || waittingQueue : " + waitingQueue.toString());
        LOGGER.info("now the RunningList : " + runningList.size() + " || runningList : " + runningList.toString());
    }
    public static class MainSchedulerUtil {
        private final static MainScheduler MAIN_SCHEDULER = new MainScheduler();
        public static MainScheduler getInstance() {
            return MAIN_SCHEDULER;
        }
    }
}
