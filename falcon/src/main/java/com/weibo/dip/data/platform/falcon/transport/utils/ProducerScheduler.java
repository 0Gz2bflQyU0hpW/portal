package com.weibo.dip.data.platform.falcon.transport.utils;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

/**
 * Created by Wen on 2017/2/20.
 */
public class ProducerScheduler {

    private final static Scheduler SCHEDULER;

    static {
        StdSchedulerFactory SchedulerFactory = new StdSchedulerFactory();

        try {
            SCHEDULER = SchedulerFactory.getScheduler();
        } catch (SchedulerException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static Scheduler getInstance() {
        return SCHEDULER;
    }

    private ProducerScheduler() {

    }

}
