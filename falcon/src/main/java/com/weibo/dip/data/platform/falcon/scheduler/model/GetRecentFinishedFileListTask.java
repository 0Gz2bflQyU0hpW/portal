package com.weibo.dip.data.platform.falcon.scheduler.model;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Date;

/**
 * Created by Wen on 2017/1/18.
 *
 */
public class GetRecentFinishedFileListTask extends Task {

    private String dataSetName;

    private String triggerName;

    private int intervalSeconds;

    private JobDataMap jobDataMap;

    private SchedulerFactory schedulerFactory = new StdSchedulerFactory();

    private Scheduler scheduler = schedulerFactory.getScheduler();

    private Class<? extends Job> className;

    private final static String GROUP_NAME = "GetRecentFinishedFileList";

    public GetRecentFinishedFileListTask() throws SchedulerException {


    }

    public String getTriggerName() {
        return triggerName;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    public int getIntervalSeconds() {
        return intervalSeconds;
    }

    public void setIntervalSeconds(int intervalSeconds) {
        this.intervalSeconds = intervalSeconds;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public Class<? extends Job> getClassName() {
        return className;
    }

    public void setClassName(Class<? extends Job> className) {
        this.className = className;
    }

    public String getDataSetName() {
        return dataSetName;
    }

    public void setDataSetName(String dataSetName) {
        this.dataSetName = dataSetName;
    }

    public JobDataMap getJobDataMap() {
        return jobDataMap;
    }

    public void setJobDataMap(JobDataMap jobDataMap) {
        this.jobDataMap = jobDataMap;
    }

    public JobDataMap createDataMap(){
        if (jobDataMap.isEmpty() || jobDataMap == null){
            jobDataMap = new JobDataMap();
            if (jobType.equals("DB")) {
//                jobDataMap.put("startTime", executeTime.getTime() / 1000 - intervalSeconds);
//                jobDataMap.put("category", dataSetName);
            }
            if (jobType.equals("HDFS")) {
                jobDataMap.put("dataSetName",dataSetName);
                jobDataMap.put("path","/user/hdfs/rawlog");
            }
        }
        return jobDataMap;
    }


    @Override
    public void setup() throws Exception {

        JobDetail jobDetail = JobBuilder.newJob(className).withIdentity(jobName,GROUP_NAME).setJobData(jobDataMap).build();
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerName, GROUP_NAME).startAt(scheduleTime)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(intervalSeconds)).build();
        scheduler.scheduleJob(jobDetail, trigger);
    }

    @Override
    public void execute() throws Exception {
        executeTime = new Date();
        scheduler.start();

    }

    @Override
    public void cleanup() throws Exception {
        endTime = new Date();
        success = true;
        scheduler.deleteJob(JobKey.jobKey(jobName));
    }

    @Override
    public int compareTo(Task o) {
        return 0;
    }
}
