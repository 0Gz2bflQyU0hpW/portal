package com.weibo.dip.data.platform.falcon.scheduler.model;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by Wen on 2017/2/16.
 *
 */
public class WorkThread implements Runnable{

    private Task task;

    private RunningList runningList = RunningList.RunningListUtil.getInstance();

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkThread.class);

    public WorkThread(Task task) {
        this.task = task;
    }

    @Override
    public void run() {
        runningList.add(task);
        task.setExecuteTime(new Date());
        try {
            task.setup();
        } catch (Exception e) {
            LOGGER.error("Exception at task.setup()." + ExceptionUtils.getFullStackTrace(e));
        }
        try {
            task.execute();
        } catch (Exception e) {
            LOGGER.error("Exception at task.execute()." + ExceptionUtils.getFullStackTrace(e));
        }
        try {
            task.cleanup();
        } catch (Exception e) {
            LOGGER.error("Exception at task.cleanup()." + ExceptionUtils.getFullStackTrace(e));
        }
        task.setEndTime(new Date());
        runningList.remove(task);
        LOGGER.info("Task : " + task.getJobName() + "has finished");
    }
}
