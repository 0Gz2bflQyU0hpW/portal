package com.weibo.dip.data.platform.falcon.kafka;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.falcon.hdfs.HDFSRawlogCollector;
import com.weibo.dip.data.platform.kafka.KafkaAdmin;
import com.weibo.dip.data.platform.kafka.KafkaWriter;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by yurun on 17/7/16.
 */
public class KafkaProducerOffsetCollect {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRawlogCollector.class);

    private static final String[] KAFKA_SERVERS = {"first.kafka.dip.weibo.com", "second.kafka.dip.weibo.com",
        "third.kafka.dip.weibo.com", "fourth.kafka.dip.weibo.com", "fifth.kafka.dip.weibo.com"};
    private static final int KAFKA_PORT = 9092;
    private static final int KAFKA_ADMIN_SO_TIMEOUT = 10000;
    private static final int KAFKA_ADMIN_BUFFERSIZE = 64 * 1024;
    private static final int KAFKA_ADMIN_THREADS = 5;

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

    private static final Map<String, Long> TOPIC_OFFSET_CACHE = new HashMap<>();

    public static class Collector implements Job {

        private SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmm00");

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            Date scheduledFileTime = context.getScheduledFireTime();

            String timestamp = timeFormat.format(scheduledFileTime);

            KafkaAdmin kafkaAdmin = null;

            try {
                kafkaAdmin = new KafkaAdmin(KAFKA_SERVERS, KAFKA_PORT, KAFKA_ADMIN_SO_TIMEOUT,
                    KAFKA_ADMIN_BUFFERSIZE);

                Map<String, Long> topicOffsets = kafkaAdmin.getTopicOffsets(KAFKA_ADMIN_THREADS);
                if (MapUtils.isEmpty(topicOffsets)) {
                    LOGGER.warn("topic offsets is empty!");

                    return;
                }

                for (Map.Entry<String, Long> entry : topicOffsets.entrySet()) {
                    String topic = entry.getKey();
                    long offset = entry.getValue();

                    if (TOPIC_OFFSET_CACHE.containsKey(topic)) {
                        long value = offset - TOPIC_OFFSET_CACHE.get(topic);

                        Map<String, Object> records = new HashMap<>();

                        records.put(TYPE, KAFKA_TYPE);
                        records.put(SERVICE, topic);
                        records.put(TIMESTAMP, scheduledFileTime.getTime());
                        records.put(VALUE, value);

                        String line = GsonUtil.toJson(records);

                        WRITER.write(line);

                        LOGGER.info(line);
                    }

                    // update cache offset
                    TOPIC_OFFSET_CACHE.put(topic, offset);
                }

                LOGGER.info("get topic offsets[{}] finished", topicOffsets.size());
            } catch (Exception e) {
                LOGGER.error("get topic offsets error: {}", ExceptionUtils.getFullStackTrace(e));
            } finally {
                if (Objects.nonNull(kafkaAdmin)) {
                    kafkaAdmin.close();
                }
            }
        }

    }

    private static final String TYPE = "type";
    private static final String SERVICE = "service";
    private static final String TIMESTAMP = "timestamp";
    private static final String VALUE = "value";

    private static final String KAFKA_TYPE = "kafka";

    public static void main(String[] args) throws Exception {
        String group = "kafka";
        String name = "collect";

        String cron = "0 0/1 * * * ?";

        JobDetail collectJob = JobBuilder.newJob(KafkaProducerOffsetCollect.Collector.class)
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
