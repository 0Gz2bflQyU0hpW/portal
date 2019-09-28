package com.weibo.dip.streaming.bean.job;

import java.util.Arrays;

/**
 * Job.
 *
 * @author yurun
 */
public class Job {
  private int jobId;
  private String name;
  private String description;
  private String submissionTime;
  private String completionTime;
  private int[] stageIds;
  private JobStatus status;
  private int numTasks;
  private int numActiveTasks;
  private int numCompletedTasks;
  private int numSkippedTasks;
  private int numFailedTasks;
  private int numActiveStages;
  private int numCompletedStages;
  private int numSkippedStages;
  private int numFailedStages;

  public Job() {}

  public int getJobId() {
    return jobId;
  }

  public void setJobId(int jobId) {
    this.jobId = jobId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getSubmissionTime() {
    return submissionTime;
  }

  public void setSubmissionTime(String submissionTime) {
    this.submissionTime = submissionTime;
  }

  public String getCompletionTime() {
    return completionTime;
  }

  public void setCompletionTime(String completionTime) {
    this.completionTime = completionTime;
  }

  public int[] getStageIds() {
    return stageIds;
  }

  public void setStageIds(int[] stageIds) {
    this.stageIds = stageIds;
  }

  public JobStatus getStatus() {
    return status;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
  }

  public int getNumTasks() {
    return numTasks;
  }

  public void setNumTasks(int numTasks) {
    this.numTasks = numTasks;
  }

  public int getNumActiveTasks() {
    return numActiveTasks;
  }

  public void setNumActiveTasks(int numActiveTasks) {
    this.numActiveTasks = numActiveTasks;
  }

  public int getNumCompletedTasks() {
    return numCompletedTasks;
  }

  public void setNumCompletedTasks(int numCompletedTasks) {
    this.numCompletedTasks = numCompletedTasks;
  }

  public int getNumSkippedTasks() {
    return numSkippedTasks;
  }

  public void setNumSkippedTasks(int numSkippedTasks) {
    this.numSkippedTasks = numSkippedTasks;
  }

  public int getNumFailedTasks() {
    return numFailedTasks;
  }

  public void setNumFailedTasks(int numFailedTasks) {
    this.numFailedTasks = numFailedTasks;
  }

  public int getNumActiveStages() {
    return numActiveStages;
  }

  public void setNumActiveStages(int numActiveStages) {
    this.numActiveStages = numActiveStages;
  }

  public int getNumCompletedStages() {
    return numCompletedStages;
  }

  public void setNumCompletedStages(int numCompletedStages) {
    this.numCompletedStages = numCompletedStages;
  }

  public int getNumSkippedStages() {
    return numSkippedStages;
  }

  public void setNumSkippedStages(int numSkippedStages) {
    this.numSkippedStages = numSkippedStages;
  }

  public int getNumFailedStages() {
    return numFailedStages;
  }

  public void setNumFailedStages(int numFailedStages) {
    this.numFailedStages = numFailedStages;
  }

  @Override
  public String toString() {
    return "Job{"
        + "jobId="
        + jobId
        + ", name='"
        + name
        + '\''
        + ", description='"
        + description
        + '\''
        + ", submissionTime='"
        + submissionTime
        + '\''
        + ", completionTime='"
        + completionTime
        + '\''
        + ", stageIds="
        + Arrays.toString(stageIds)
        + ", status="
        + status
        + ", numTasks="
        + numTasks
        + ", numActiveTasks="
        + numActiveTasks
        + ", numCompletedTasks="
        + numCompletedTasks
        + ", numSkippedTasks="
        + numSkippedTasks
        + ", numFailedTasks="
        + numFailedTasks
        + ", numActiveStages="
        + numActiveStages
        + ", numCompletedStages="
        + numCompletedStages
        + ", numSkippedStages="
        + numSkippedStages
        + ", numFailedStages="
        + numFailedStages
        + '}';
  }
}
