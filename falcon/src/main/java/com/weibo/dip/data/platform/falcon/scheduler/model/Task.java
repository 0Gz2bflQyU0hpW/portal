package com.weibo.dip.data.platform.falcon.scheduler.model;

import java.util.Date;

/**
 * Created by Wen on 2017/1/18.
 *
 */
public abstract class Task implements Comparable<Task>{

    protected String uuid;

    protected int jobID;

    protected int priority;

    protected long delayFactor = System.currentTimeMillis();

    protected String scheduleJobType;

    protected String jobName;

    protected String jobType;

    protected Date scheduleTime;

    protected Date executeTime;

    protected Date endTime;

    protected boolean success;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public int getJobID() {
        return jobID;
    }

    public void setJobID(int jobID) {
        this.jobID = jobID;
    }

    public int getPriority() {
        return priority;
    }

    public long getDelayFactor() {
        return delayFactor;
    }

    public String getScheduleJobType() {
        return scheduleJobType;
    }

    public String getJobName() {
        return jobName;
    }

    public String getJobType() {
        return jobType;
    }

    public Date getScheduleTime() {
        return scheduleTime;
    }

    public Date getExecuteTime() {
        return executeTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public void setDelayFactor(long delayFactor) {
        this.delayFactor = delayFactor;
    }

    public void setScheduleJobType(String scheduleJobType) {
        this.scheduleJobType = scheduleJobType;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public void setScheduleTime(Date scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public void setExecuteTime(Date executeTime) {
        this.executeTime = executeTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public abstract void setup() throws Exception;

    public abstract void execute() throws Exception;

    public abstract void cleanup() throws Exception;
}
