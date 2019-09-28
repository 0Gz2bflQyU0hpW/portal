package com.weibo.dip.data.platform.falcon.scheduler.model;

import com.weibo.dip.data.platform.falcon.scheduler.util.ConfigUtils;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Created by Wen on 2017/2/16.
 *
 */
public class WorkerPool {
    private ConcurrentHashMap<String,ThreadPoolExecutor> poolMap = new ConcurrentHashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerPool.class);
    private WorkerPool() {

    }

    public void prepare(String[] jobTypes) {
        if (ArrayUtils.isEmpty(jobTypes)) {
            LOGGER.error("The JobTypes is empty or null.Prepare failed!");
        }
        else {
            for (String jobType : jobTypes) {
                poolMap.put(jobType,new ThreadPoolExecutor(ConfigUtils.getThreadNum(jobType), ConfigUtils.getThreadNum(jobType), 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()));
                LOGGER.info("JobType: " + jobType + "has finished ThreadPool");
            }
        }
    }

    public void execute(com.weibo.dip.data.platform.falcon.scheduler.model.Task task) {
        String jobType = task.getJobType();
        if (poolMap.get(jobType) == null) {
            LOGGER.error("This jobType: " + jobType + "doesn't support!");
        }
        else {
            poolMap.get(jobType).execute(new com.weibo.dip.data.platform.falcon.scheduler.model.WorkThread(task));
        }
    }

    public static class WorkerPoolUtil {
        private static final WorkerPool WORKER_POOL = new WorkerPool();
        public static WorkerPool getInstance() {
            return WORKER_POOL;
        }
    }
}
