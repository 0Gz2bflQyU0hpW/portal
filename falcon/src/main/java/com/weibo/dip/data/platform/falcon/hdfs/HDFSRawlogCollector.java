package com.weibo.dip.data.platform.falcon.hdfs;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import com.weibo.dip.data.platform.kafka.KafkaWriter;
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
 * Created by yurun on 17/5/17.
 */
public class HDFSRawlogCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRawlogCollector.class);

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

    private static final String GODEYES_SERVER_KAFKA = "10.13.4.44:9092";
    private static final String GODEYES_SERVER_TOPIC = "godeyes_collect";

    private static final KafkaWriter WRITER;

    static {
        WRITER = new KafkaWriter(GODEYES_SERVER_KAFKA, GODEYES_SERVER_TOPIC);
    }

    public static class Collector implements Job {

        private SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmm00");

        private SimpleDateFormat hdfsDayFormat = new SimpleDateFormat("yyyy_MM_dd");

        private List<Path> getDatasetPaths() throws Exception {
            return HDFSUtil.listDirs(RAWLOG_PREFIX, false);
        }

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            Date scheduledFileTime = context.getScheduledFireTime();

            String timestamp = timeFormat.format(scheduledFileTime);

            String dayDir = hdfsDayFormat.format(scheduledFileTime);

            try {
                List<Path> datasetPaths = getDatasetPaths();
                if (CollectionUtils.isEmpty(datasetPaths)) {
                    LOGGER.warn("dataset path is empty!");

                    return;
                }

                for (Path datasetPath : datasetPaths) {
                    long length = 0;

                    ContentSummary summary = HDFSUtil.summary(new Path(datasetPath, dayDir));
                    if (Objects.nonNull(summary)) {
                        length = summary.getLength();
                    }

                    Map<String, Object> records = new HashMap<>();

                    records.put(TYPE, HDFS_TYPE);
                    records.put(SERVICE, datasetPath.getName());
                    records.put(TIMESTAMP, scheduledFileTime.getTime());
                    records.put(VALUE, length);

                    String line = GsonUtil.toJson(records);

                    WRITER.write(line);

                    LOGGER.info(line);
                }

                LOGGER.info("dataset {} collect finished", timestamp);
            } catch (Exception e) {
                LOGGER.error("dataset {} collect error: {}", timestamp, ExceptionUtils.getFullStackTrace(e));
            }
        }

    }

    private static final String TYPE = "type";
    private static final String SERVICE = "service";
    private static final String TIMESTAMP = "timestamp";
    private static final String VALUE = "value";

    private static final String HDFS_TYPE = "hdfs";

    public static void main(String[] args) throws Exception {
        String group = "hdfs";
        String name = "collect";

        String cron = "0 0/5 * * * ?";

        JobDetail collectJob = JobBuilder.newJob(Collector.class)
            .withIdentity(name, group)
            .build();

        Trigger collectTrigger = TriggerBuilder.newTrigger()
            .withIdentity(name, group)
            .withSchedule(CronScheduleBuilder.cronSchedule(cron))
            .build();

        SCHEDULER.scheduleJob(collectJob, collectTrigger);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            try {
                SCHEDULER.shutdown(true);
                LOGGER.info("collect scheduler alread shutdown");

                WRITER.close();
                LOGGER.info("kafka writer closed");
            } catch (Exception e) {
                LOGGER.error("quartz scheduler shutdown error: " + ExceptionUtils.getFullStackTrace(e));
            }

        }));
    }

}
