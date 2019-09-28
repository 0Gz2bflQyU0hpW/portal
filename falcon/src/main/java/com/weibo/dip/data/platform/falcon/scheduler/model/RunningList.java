package com.weibo.dip.data.platform.falcon.scheduler.model;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Wen on 2017/2/16.
 *  Every JobType has only one RunningList.
 */
public class RunningList {

    private ConcurrentHashMap<String,List<Task>> runningMap = new ConcurrentHashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(RunningList.class);

    private RunningList() {

    }

    public void prepare(String[] jobTypes) {
        if (ArrayUtils.isEmpty(jobTypes)) {
            LOGGER.error("The JobTypes is empty or null.Prepare failed!");
        }
        else {
            for (String jobType : jobTypes) {
                if (CollectionUtils.isEmpty(runningMap.get(jobType))) {
                    runningMap.put(jobType,new ArrayList<>());
                }
            }
        }
    }

    public void add(Task task) {
        String jobType = task.getJobType();
        if (runningMap.get(jobType) == null) {
            LOGGER.error("This jobType: " + jobType + "doesn't support!");
        }
        List<Task> list = runningMap.get(jobType);
        synchronized (list) {  //?
            list.add(task);
        }
    }

    public void remove(Task task) {
        String jobType = task.getJobType();
        if (runningMap.get(jobType)==null) {
            LOGGER.error("This jobType: " + jobType + "doesn't support!");
        }
        List<Task> list = runningMap.get(jobType);
        synchronized (list) {
            list.remove(task);
        }
    }

    public int size() {
        int size = 0;
        for (String jobType : runningMap.keySet()) {
            size += runningMap.get(jobType).size();
        }
        return size;
    }

    public int size(String jobType) {
        if (CollectionUtils.isEmpty(runningMap.get(jobType))) {
            return 0;
        }
        else {
            return runningMap.get(jobType).size();
        }
    }

    public static class RunningListUtil {
        private final static RunningList RUNNING_LIST = new RunningList();
        public static RunningList getInstance() {
            return RUNNING_LIST;
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (String jobType : runningMap.keySet()) {
            sb.append(jobType + " : " + runningMap.get(jobType));
        }
        return sb.toString();
    }
}
