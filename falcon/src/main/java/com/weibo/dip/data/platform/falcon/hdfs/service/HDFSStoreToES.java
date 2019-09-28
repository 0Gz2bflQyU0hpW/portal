package com.weibo.dip.data.platform.falcon.hdfs.service;

import com.weibo.dip.data.platform.commons.record.Record;
import com.weibo.dip.data.platform.commons.record.RecordStore;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by shixi_dongxue3 on 2018/2/7.
 */
public class HDFSStoreToES {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSStoreToES.class);

    private static final String RAWLOG_PREFIX = "/user/hdfs/rawlog/";

    private static final SchedulerFactory SCHEDULER_FACTORY = new StdSchedulerFactory();

    private static final Scheduler SCHEDULER;

    static {
        try {
            SCHEDULER = SCHEDULER_FACTORY.getScheduler();

            SCHEDULER.start();

        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static class StoreToES implements Job {
        private SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:00");

        private List<Path> getDatasetPaths() throws Exception {
            return HDFSUtil.listDirs(RAWLOG_PREFIX, false);
        }

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            Date scheduledFileTime = context.getScheduledFireTime();

            String timestamp = timeFormat.format(scheduledFileTime);

            List<Record> records = new ArrayList<Record>();

            try {
                List<Path> datasetPaths = getDatasetPaths();
                if (CollectionUtils.isEmpty(datasetPaths)) {
                    LOGGER.warn("dataset path is empty!");

                    return;
                }

                for (Path datasetPath : datasetPaths) {
                    long length = 0;

                    ContentSummary summary = HDFSUtil.summary(datasetPath);
                    if (Objects.nonNull(summary)) {
                        length = summary.getLength();
                    }

                    Record record = new Record();

                    record.setBusiness("dip-monitoring-hdfs");
                    record.setTimestamp(scheduledFileTime.getTime());
                    record.addDimension("category", datasetPath.getName());
                    record.addMetric("size",length);

                    records.add(record);

                    String line = GsonUtil.toJson(record);

                    LOGGER.info(line);
                }

                RecordStore.store(records);

                LOGGER.info("dataset {} store to ES finished", timestamp);
            } catch (Exception e) {
                LOGGER.error("dataset {} store to ES error: {}", timestamp, ExceptionUtils.getFullStackTrace(e));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        String name = "storeToES";
        String group = "hdfs";

        String cron = "0 0/5 * * * ?";

        JobDetail storeJob = JobBuilder.newJob(StoreToES.class)
                .withIdentity(name, group)
                .build();

        Trigger storeTrigger = TriggerBuilder.newTrigger()
                .withIdentity(name, group)
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .build();

        SCHEDULER.scheduleJob(storeJob, storeTrigger);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            try {
                SCHEDULER.shutdown(true);
                LOGGER.info("store scheduler alread shutdown");
            } catch (Exception e) {
                LOGGER.error("quartz scheduler shutdown error: " + ExceptionUtils.getFullStackTrace(e));
            }

        }));
    }
}
