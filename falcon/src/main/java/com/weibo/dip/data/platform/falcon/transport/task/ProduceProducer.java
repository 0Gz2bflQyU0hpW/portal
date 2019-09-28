package com.weibo.dip.data.platform.falcon.transport.task;

import com.weibo.dip.data.platform.falcon.transport.utils.ConfigUtils;
import com.weibo.dip.data.platform.falcon.transport.utils.ProducerScheduler;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Wen on 2017/3/9.
 */
public class ProduceProducer extends Producer{
    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    private Worker worker = new Worker();

    private class Worker {

        private Scheduler scheduler;

        Worker() {
            scheduler = ProducerScheduler.getInstance();
        }

        public void start() throws SchedulerException {
            JobDetail jobDetail = JobBuilder.newJob(ProducerTask.class).build();

            Trigger trigger = TriggerBuilder.newTrigger()
                    .startNow()
                    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInMilliseconds(ConfigUtils.PRODUCER_GET_INTERVAL)
                            .repeatForever())
                    .build();

            scheduler.scheduleJob(jobDetail, trigger);
            scheduler.start();
        }

        public void stop() throws SchedulerException {
            scheduler.shutdown(true);
        }

    }

    @Override
    public void start() throws Exception {
        worker.start();
    }

    @Override
    public void stop() throws Exception {
        worker.stop();
    }
}
