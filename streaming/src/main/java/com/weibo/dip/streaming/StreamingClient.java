package com.weibo.dip.streaming;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import com.weibo.dip.streaming.bean.EventRate;
import com.weibo.dip.streaming.bean.Executor;
import com.weibo.dip.streaming.bean.Receiver;
import com.weibo.dip.streaming.bean.Statistic;
import com.weibo.dip.streaming.bean.application.Application;
import com.weibo.dip.streaming.bean.application.ApplicationAttempt;
import com.weibo.dip.streaming.bean.application.ApplicationStatus;
import com.weibo.dip.streaming.bean.batch.Batch;
import com.weibo.dip.streaming.bean.batch.BatchStatus;
import com.weibo.dip.streaming.bean.job.Job;
import com.weibo.dip.streaming.bean.job.JobStatus;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming client.
 *
 * @author yurun
 */
public class StreamingClient implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingClient.class);

  private static final String SPARK = "SPARK";
  private static final String QUEUED = "QUEUED";
  private static final String PROCESSING = "PROCESSING";
  private static final String COMPLETED = "COMPLETED";

  private static final String APPLICATIONS_REST_API = "%sapi/v1/applications";
  private static final String APPLICATIONS_STATUS_REST_API = APPLICATIONS_REST_API + "?status=%s";

  private static final String REST_API = "%sapi/v1/applications/%s/";

  private static final String JOBS_REST_API = REST_API + "jobs";
  private static final String JOBS_STATUS_REST_API = JOBS_REST_API + "?status=%s";

  private static final String EXECUTORS_REST_API = REST_API + "executors";
  private static final String ALLEXECUTORS_REST_API = REST_API + "allexecutors";

  private static final String STATISTICS_REST_API = REST_API + "streaming/statistics";

  private static final String RECEIVERS_REST_API = REST_API + "streaming/receivers";

  private static final String BATCHES_REST_API = REST_API + "streaming/batches";
  private static final String BATCHES_STATUS_REST_API = BATCHES_REST_API + "?status=%s";

  private YarnClient yarnClient;

  private JsonParser parser;

  /** Construct a instance. */
  public StreamingClient() {
    yarnClient = YarnClient.createYarnClient();

    yarnClient.init(new Configuration());
    yarnClient.start();

    parser = new JsonParser();
  }

  public EnumSet<YarnApplicationState> getAliveStates() {
    EnumSet<YarnApplicationState> aliveStates = EnumSet.noneOf(YarnApplicationState.class);
    aliveStates.add(YarnApplicationState.SUBMITTED);
    aliveStates.add(YarnApplicationState.ACCEPTED);
    aliveStates.add(YarnApplicationState.RUNNING);
    return aliveStates;
  }

  public EnumSet<YarnApplicationState> getAllStates() {
    EnumSet<YarnApplicationState> allStates = getAliveStates();
    allStates.add(YarnApplicationState.FAILED);
    return allStates;
  }

  public List<ApplicationReport> getApplications(EnumSet<YarnApplicationState> states) throws Exception {
    List<ApplicationReport> applicationReports = null;
    if (states != null && !states.isEmpty()) {
      Set<String> set = new HashSet<>();
      set.add(SPARK);

      applicationReports = yarnClient.getApplications(set, states);
    }
    return applicationReports;
  }

  public ApplicationReport getApplication(EnumSet<YarnApplicationState> states, String appName) {
    List<ApplicationReport> applicationReports = null;
    try {
      applicationReports = getApplications(states);
    } catch (Exception e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }

    if (applicationReports == null) {
      LOGGER.info("couldn't find application {} on yarn.", appName);
      return null;
    }
    ApplicationReport applicationReport = null;
    for (ApplicationReport app : applicationReports) {
      if (appName.equals(app.getName())) {
        applicationReport = app;
      }
    }
    return applicationReport;
  }

  /**
   * Get yarn application report of the specified name.
   *
   * @param appName application name
   * @return application report
   * @throws Exception if access yarn error
   */
  public ApplicationReport getApplicationReport(String appName) throws Exception {
    List<ApplicationReport> applicationReports =
        yarnClient.getApplications(
            Collections.singleton(SPARK), EnumSet.of(YarnApplicationState.RUNNING));
    if (CollectionUtils.isEmpty(applicationReports)) {
      return null;
    }

    for (ApplicationReport applicationReport : applicationReports) {
      if (applicationReport.getName().equals(appName)) {
        return applicationReport;
      }
    }

    return null;
  }

  public boolean isRunning(String appName) throws Exception {
    return Objects.nonNull(getApplicationReport(appName));
  }

  /**
   * Get applications of the specified name and status.
   *
   * @param appName application name
   * @param applicationStatus application status
   * @return applications
   * @throws Exception if get or parse error
   */
  public List<Application> getApplications(String appName, ApplicationStatus applicationStatus)
      throws Exception {
    ApplicationReport applicationReport = getApplicationReport(appName);
    Preconditions.checkState(
        Objects.nonNull(applicationReport), "Application %s is not running on yarn", appName);

    String response =
        HttpClientUtil.doGet(
            String.format(
                APPLICATIONS_STATUS_REST_API,
                applicationReport.getTrackingUrl(),
                applicationStatus.name()));

    JsonArray applicationJsonObjs = parser.parse(response).getAsJsonArray();
    if (applicationJsonObjs.size() == 0) {
      return null;
    }

    List<Application> applications = new ArrayList<>();

    for (JsonElement applicationJsonElement : applicationJsonObjs) {
      JsonObject applicationJsonOjb = applicationJsonElement.getAsJsonObject();

      Application application = new Application();

      application.setId(applicationJsonOjb.getAsJsonPrimitive("id").getAsString());
      application.setName(applicationJsonOjb.getAsJsonPrimitive("name").getAsString());

      JsonArray applicationAttempJsonObjs = applicationJsonOjb.getAsJsonArray("attempts");
      if (applicationAttempJsonObjs.size() == 0) {
        application.setAttempts(null);
      } else {
        List<ApplicationAttempt> applicationAttempts = new ArrayList<>();

        for (JsonElement applicationAttemptJsonElement : applicationAttempJsonObjs) {
          JsonObject applicationAttemptJsonObj = applicationAttemptJsonElement.getAsJsonObject();

          ApplicationAttempt applicationAttempt = new ApplicationAttempt();

          applicationAttempt.setStartTime(
              applicationAttemptJsonObj.getAsJsonPrimitive("startTime").getAsString());
          applicationAttempt.setEndTime(
              applicationAttemptJsonObj.getAsJsonPrimitive("endTime").getAsString());
          applicationAttempt.setLastUpdated(
              applicationAttemptJsonObj.getAsJsonPrimitive("lastUpdated").getAsString());
          applicationAttempt.setDuration(
              applicationAttemptJsonObj.getAsJsonPrimitive("duration").getAsInt());
          applicationAttempt.setSparkUser(
              applicationAttemptJsonObj.getAsJsonPrimitive("sparkUser").getAsString());
          applicationAttempt.setCompleted(
              applicationAttemptJsonObj.getAsJsonPrimitive("completed").getAsBoolean());
          applicationAttempt.setEndTimeEpoch(
              applicationAttemptJsonObj.getAsJsonPrimitive("endTimeEpoch").getAsLong());
          applicationAttempt.setLastUpdatedEpoch(
              applicationAttemptJsonObj.getAsJsonPrimitive("lastUpdatedEpoch").getAsLong());
          applicationAttempt.setStartTimeEpoch(
              applicationAttemptJsonObj.getAsJsonPrimitive("startTimeEpoch").getAsLong());

          applicationAttempts.add(applicationAttempt);
        }

        application.setAttempts(applicationAttempts);
      }

      applications.add(application);
    }

    return applications;
  }

  /**
   * Get jobs of the specifiled name and status.
   *
   * @param appName application name
   * @param jobStatus job status
   * @return jobs
   * @throws Exception if get or parse error
   */
  public List<Job> getJobs(String appName, JobStatus jobStatus) throws Exception {
    ApplicationReport applicationReport = getApplicationReport(appName);
    Preconditions.checkState(
        Objects.nonNull(applicationReport), "Application %s is not running on yarn", appName);

    String response =
        HttpClientUtil.doGet(
            String.format(
                JOBS_STATUS_REST_API,
                applicationReport.getTrackingUrl(),
                applicationReport.getApplicationId().toString(),
                jobStatus.name()));

    JsonArray jobJsonObjs = parser.parse(response).getAsJsonArray();
    if (jobJsonObjs.size() == 0) {
      return null;
    }

    List<Job> jobs = new ArrayList<>();

    for (JsonElement jobJsonElement : jobJsonObjs) {
      JsonObject jobJsonObj = jobJsonElement.getAsJsonObject();

      Job job = new Job();

      job.setJobId(jobJsonObj.getAsJsonPrimitive("jobId").getAsInt());
      job.setName(jobJsonObj.getAsJsonPrimitive("name").getAsString());

      if (jobJsonObj.has("description")) {
        job.setDescription(jobJsonObj.getAsJsonPrimitive("description").getAsString());
      }

      job.setSubmissionTime(jobJsonObj.getAsJsonPrimitive("submissionTime").getAsString());
      if (jobJsonObj.has("completionTime")) {
        job.setCompletionTime(jobJsonObj.getAsJsonPrimitive("completionTime").getAsString());
      }

      JsonArray stageIdObjs = jobJsonObj.getAsJsonArray("stageIds");

      int[] stageIds = new int[stageIdObjs.size()];

      for (int index = 0; index < stageIdObjs.size(); index++) {
        stageIds[index] = stageIdObjs.get(index).getAsInt();
      }

      job.setStageIds(stageIds);
      job.setStatus(jobStatus);
      job.setNumTasks(jobJsonObj.getAsJsonPrimitive("numTasks").getAsInt());
      job.setNumActiveTasks(jobJsonObj.getAsJsonPrimitive("numActiveTasks").getAsInt());
      job.setNumCompletedTasks(jobJsonObj.getAsJsonPrimitive("numCompletedTasks").getAsInt());
      job.setNumSkippedTasks(jobJsonObj.getAsJsonPrimitive("numSkippedTasks").getAsInt());
      job.setNumFailedTasks(jobJsonObj.getAsJsonPrimitive("numFailedTasks").getAsInt());
      job.setNumActiveStages(jobJsonObj.getAsJsonPrimitive("numActiveStages").getAsInt());
      job.setNumCompletedStages(jobJsonObj.getAsJsonPrimitive("numCompletedStages").getAsInt());
      job.setNumSkippedStages(jobJsonObj.getAsJsonPrimitive("numSkippedStages").getAsInt());
      job.setNumFailedStages(jobJsonObj.getAsJsonPrimitive("numFailedStages").getAsInt());

      jobs.add(job);
    }

    return jobs;
  }

  /**
   * Get all active executors of the specified name.
   *
   * @param appName application name
   * @return executors
   * @throws Exception if get or parse error
   */
  public List<Executor> getExecutors(String appName) throws Exception {
    ApplicationReport applicationReport = getApplicationReport(appName);
    Preconditions.checkState(
        Objects.nonNull(applicationReport), "Application %s is not running on yarn", appName);

    String response =
        HttpClientUtil.doGet(
            String.format(
                EXECUTORS_REST_API,
                applicationReport.getTrackingUrl(),
                applicationReport.getApplicationId().toString()));

    JsonArray executorJsonObjs = parser.parse(response).getAsJsonArray();
    if (executorJsonObjs.size() == 0) {
      return null;
    }

    List<Executor> executors = new ArrayList<>();

    for (JsonElement executorJsonElement : executorJsonObjs) {
      JsonObject executorJsonObj = executorJsonElement.getAsJsonObject();

      Executor executor = new Executor();

      executor.setId(executorJsonObj.getAsJsonPrimitive("id").getAsString());
      executor.setHostPort(executorJsonObj.getAsJsonPrimitive("hostPort").getAsString());
      executor.setActive(executorJsonObj.getAsJsonPrimitive("isActive").getAsBoolean());
      executor.setRddBlocks(executorJsonObj.getAsJsonPrimitive("rddBlocks").getAsInt());
      executor.setMemoryUsed(executorJsonObj.getAsJsonPrimitive("memoryUsed").getAsLong());
      executor.setDiskUsed(executorJsonObj.getAsJsonPrimitive("diskUsed").getAsLong());
      executor.setTotalCores(executorJsonObj.getAsJsonPrimitive("totalCores").getAsInt());
      executor.setMaxTasks(executorJsonObj.getAsJsonPrimitive("maxTasks").getAsInt());
      executor.setActiveTasks(executorJsonObj.getAsJsonPrimitive("activeTasks").getAsInt());
      executor.setFailedTasks(executorJsonObj.getAsJsonPrimitive("failedTasks").getAsInt());
      executor.setCompletedTasks(executorJsonObj.getAsJsonPrimitive("completedTasks").getAsInt());
      executor.setTotalTasks(executorJsonObj.getAsJsonPrimitive("totalTasks").getAsInt());
      executor.setTotalDuration(executorJsonObj.getAsJsonPrimitive("totalDuration").getAsLong());
      executor.setTotalGcTime(executorJsonObj.getAsJsonPrimitive("totalGCTime").getAsLong());
      executor.setTotalInputBytes(
          executorJsonObj.getAsJsonPrimitive("totalInputBytes").getAsLong());
      executor.setTotalShuffleRead(
          executorJsonObj.getAsJsonPrimitive("totalShuffleRead").getAsLong());
      executor.setTotalShuffleWrite(
          executorJsonObj.getAsJsonPrimitive("totalShuffleWrite").getAsLong());
      executor.setBlacklisted(executorJsonObj.getAsJsonPrimitive("isBlacklisted").getAsBoolean());
      executor.setMaxMemory(executorJsonObj.getAsJsonPrimitive("maxMemory").getAsLong());
      executor.setStdout(
          executorJsonObj
              .getAsJsonObject("executorLogs")
              .getAsJsonPrimitive("stdout")
              .getAsString());
      executor.setStderr(
          executorJsonObj
              .getAsJsonObject("executorLogs")
              .getAsJsonPrimitive("stderr")
              .getAsString());
      executor.setUsedOnHeapStorageMemory(
          executorJsonObj
              .getAsJsonObject("memoryMetrics")
              .getAsJsonPrimitive("usedOnHeapStorageMemory")
              .getAsLong());
      executor.setUsedOffHeapStorageMemory(
          executorJsonObj
              .getAsJsonObject("memoryMetrics")
              .getAsJsonPrimitive("usedOffHeapStorageMemory")
              .getAsLong());
      executor.setTotalOnHeapStorageMemory(
          executorJsonObj
              .getAsJsonObject("memoryMetrics")
              .getAsJsonPrimitive("totalOnHeapStorageMemory")
              .getAsLong());
      executor.setTotalOffHeapStorageMemory(
          executorJsonObj
              .getAsJsonObject("memoryMetrics")
              .getAsJsonPrimitive("totalOffHeapStorageMemory")
              .getAsLong());

      executors.add(executor);
    }

    return executors;
  }

  /**
   * Get all(active and dead) executors of the specified name.
   *
   * @param appName application name
   * @return executors
   * @throws Exception if get or parse error
   */
  public List<Executor> getAllExecutors(String appName) throws Exception {
    ApplicationReport applicationReport = getApplicationReport(appName);
    Preconditions.checkState(
        Objects.nonNull(applicationReport), "Application %s is not running on yarn", appName);

    String response =
        HttpClientUtil.doGet(
            String.format(
                ALLEXECUTORS_REST_API,
                applicationReport.getTrackingUrl(),
                applicationReport.getApplicationId().toString()));

    JsonArray executorJsonObjs = parser.parse(response).getAsJsonArray();
    if (executorJsonObjs.size() == 0) {
      return null;
    }

    List<Executor> executors = new ArrayList<>();

    for (JsonElement executorJsonElement : executorJsonObjs) {
      JsonObject executorJsonObj = executorJsonElement.getAsJsonObject();

      Executor executor = new Executor();

      executor.setId(executorJsonObj.getAsJsonPrimitive("id").getAsString());
      executor.setHostPort(executorJsonObj.getAsJsonPrimitive("hostPort").getAsString());
      executor.setActive(executorJsonObj.getAsJsonPrimitive("isActive").getAsBoolean());
      executor.setRddBlocks(executorJsonObj.getAsJsonPrimitive("rddBlocks").getAsInt());
      executor.setMemoryUsed(executorJsonObj.getAsJsonPrimitive("memoryUsed").getAsLong());
      executor.setDiskUsed(executorJsonObj.getAsJsonPrimitive("diskUsed").getAsLong());
      executor.setTotalCores(executorJsonObj.getAsJsonPrimitive("totalCores").getAsInt());
      executor.setMaxTasks(executorJsonObj.getAsJsonPrimitive("maxTasks").getAsInt());
      executor.setActiveTasks(executorJsonObj.getAsJsonPrimitive("activeTasks").getAsInt());
      executor.setFailedTasks(executorJsonObj.getAsJsonPrimitive("failedTasks").getAsInt());
      executor.setCompletedTasks(executorJsonObj.getAsJsonPrimitive("completedTasks").getAsInt());
      executor.setTotalTasks(executorJsonObj.getAsJsonPrimitive("totalTasks").getAsInt());
      executor.setTotalDuration(executorJsonObj.getAsJsonPrimitive("totalDuration").getAsLong());
      executor.setTotalGcTime(executorJsonObj.getAsJsonPrimitive("totalGCTime").getAsLong());
      executor.setTotalInputBytes(
          executorJsonObj.getAsJsonPrimitive("totalInputBytes").getAsLong());
      executor.setTotalShuffleRead(
          executorJsonObj.getAsJsonPrimitive("totalShuffleRead").getAsLong());
      executor.setTotalShuffleWrite(
          executorJsonObj.getAsJsonPrimitive("totalShuffleWrite").getAsLong());
      executor.setBlacklisted(executorJsonObj.getAsJsonPrimitive("isBlacklisted").getAsBoolean());
      executor.setMaxMemory(executorJsonObj.getAsJsonPrimitive("maxMemory").getAsLong());
      executor.setStdout(
          executorJsonObj
              .getAsJsonObject("executorLogs")
              .getAsJsonPrimitive("stdout")
              .getAsString());
      executor.setStderr(
          executorJsonObj
              .getAsJsonObject("executorLogs")
              .getAsJsonPrimitive("stderr")
              .getAsString());
      executor.setUsedOnHeapStorageMemory(
          executorJsonObj
              .getAsJsonObject("memoryMetrics")
              .getAsJsonPrimitive("usedOnHeapStorageMemory")
              .getAsLong());
      executor.setUsedOffHeapStorageMemory(
          executorJsonObj
              .getAsJsonObject("memoryMetrics")
              .getAsJsonPrimitive("usedOffHeapStorageMemory")
              .getAsLong());
      executor.setTotalOnHeapStorageMemory(
          executorJsonObj
              .getAsJsonObject("memoryMetrics")
              .getAsJsonPrimitive("totalOnHeapStorageMemory")
              .getAsLong());
      executor.setTotalOffHeapStorageMemory(
          executorJsonObj
              .getAsJsonObject("memoryMetrics")
              .getAsJsonPrimitive("totalOffHeapStorageMemory")
              .getAsLong());

      executors.add(executor);
    }

    return executors;
  }

  /**
   * Get statistics of the specified name.
   *
   * @param appName application name
   * @return statistics
   * @throws Exception if get or parse error
   */
  public Statistic getStatistic(String appName) throws Exception {
    ApplicationReport applicationReport = getApplicationReport(appName);
    Preconditions.checkState(
        Objects.nonNull(applicationReport), "Application %s is not running on yarn", appName);

    String response =
        HttpClientUtil.doGet(
            String.format(
                STATISTICS_REST_API,
                applicationReport.getTrackingUrl(),
                applicationReport.getApplicationId().toString()));

    JsonObject statisticJsonObj = parser.parse(response).getAsJsonObject();

    Statistic statistic = new Statistic();

    statistic.setStartTime(statisticJsonObj.getAsJsonPrimitive("startTime").getAsString());
    statistic.setBatchDuration(statisticJsonObj.getAsJsonPrimitive("batchDuration").getAsInt());
    statistic.setNumReceivers(statisticJsonObj.getAsJsonPrimitive("numReceivers").getAsInt());
    statistic.setNumActiveReceivers(
        statisticJsonObj.getAsJsonPrimitive("numActiveReceivers").getAsInt());
    statistic.setNumInactiveReceivers(
        statisticJsonObj.getAsJsonPrimitive("numInactiveReceivers").getAsInt());
    statistic.setNumTotalCompletedBatches(
        statisticJsonObj.getAsJsonPrimitive("numTotalCompletedBatches").getAsInt());
    statistic.setNumRetainedCompletedBatches(
        statisticJsonObj.getAsJsonPrimitive("numRetainedCompletedBatches").getAsInt());
    statistic.setNumActiveBatches(
        statisticJsonObj.getAsJsonPrimitive("numActiveBatches").getAsInt());
    statistic.setNumProcessedRecords(
        statisticJsonObj.getAsJsonPrimitive("numProcessedRecords").getAsInt());
    statistic.setNumReceivedRecords(
        statisticJsonObj.getAsJsonPrimitive("numReceivedRecords").getAsInt());
    statistic.setAvgInputRate(statisticJsonObj.getAsJsonPrimitive("avgInputRate").getAsDouble());
    statistic.setAvgSchedulingDelay(
        statisticJsonObj.getAsJsonPrimitive("avgSchedulingDelay").getAsInt());
    if (statisticJsonObj.has("avgProcessingTime")) {
      statistic.setAvgProcessingTime(
          statisticJsonObj.getAsJsonPrimitive("avgProcessingTime").getAsInt());
    }
    if (statisticJsonObj.has("avgTotalDelay")) {
      statistic.setAvgTotalDelay(statisticJsonObj.getAsJsonPrimitive("avgTotalDelay").getAsInt());
    }


    return statistic;
  }

  /**
   * Get receivers of the specified name.
   *
   * @param appName application name
   * @return receivers
   * @throws Exception if get or parse error
   */
  public List<Receiver> getReceivers(String appName) throws Exception {
    ApplicationReport applicationReport = getApplicationReport(appName);
    Preconditions.checkState(
        Objects.nonNull(applicationReport), "Application %s is not running on yarn", appName);

    String response =
        HttpClientUtil.doGet(
            String.format(
                RECEIVERS_REST_API,
                applicationReport.getTrackingUrl(),
                applicationReport.getApplicationId().toString()));

    JsonArray receiverJsonObjs = parser.parse(response).getAsJsonArray();

    List<Receiver> receivers = new ArrayList<>();

    for (JsonElement receiverJsonElement : receiverJsonObjs) {
      JsonObject receiverJsonObj = receiverJsonElement.getAsJsonObject();

      Receiver receiver = new Receiver();

      receiver.setStreamId(receiverJsonObj.getAsJsonPrimitive("streamId").getAsInt());
      receiver.setStreamName(receiverJsonObj.getAsJsonPrimitive("streamName").getAsString());
      receiver.setActive(receiverJsonObj.getAsJsonPrimitive("isActive").getAsBoolean());
      receiver.setExecutorId(receiverJsonObj.getAsJsonPrimitive("executorId").getAsString());
      receiver.setExecutorHost(receiverJsonObj.getAsJsonPrimitive("executorHost").getAsString());
      receiver.setAvgEventRate(receiverJsonObj.getAsJsonPrimitive("avgEventRate").getAsDouble());

      JsonArray eventRateJsonObjs = receiverJsonObj.getAsJsonArray("eventRates");
      if (eventRateJsonObjs.size() == 0) {
        receiver.setEventRates(null);
      } else {
        List<EventRate> eventRates = new ArrayList<>();

        for (JsonElement eventRateJsonElement : eventRateJsonObjs) {
          JsonArray eventRateJsonObj = eventRateJsonElement.getAsJsonArray();

          EventRate eventRate = new EventRate();

          eventRate.setTimestamp(eventRateJsonObj.get(0).getAsLong());
          eventRate.setRate(eventRateJsonObj.get(1).getAsDouble());

          eventRates.add(eventRate);
        }

        receiver.setEventRates(eventRates);
      }

      receivers.add(receiver);
    }

    return receivers;
  }

  /**
   * Get batches of the specified name and status.
   *
   * @param appName application name
   * @param batchStatus batch status
   * @return batched
   * @throws Exception if get or parse error
   */
  public List<Batch> getBatchs(String appName, BatchStatus batchStatus) throws Exception {
    ApplicationReport applicationReport = getApplicationReport(appName);
    Preconditions.checkState(
        Objects.nonNull(applicationReport), "Application %s is not running on yarn", appName);

    String response =
        HttpClientUtil.doGet(
            String.format(
                BATCHES_STATUS_REST_API,
                applicationReport.getTrackingUrl(),
                applicationReport.getApplicationId().toString(),
                batchStatus.name()));

    JsonArray batchJsonObjs = parser.parse(response).getAsJsonArray();

    List<Batch> batches = new ArrayList<>();

    for (JsonElement batchJsonElement : batchJsonObjs) {
      JsonObject jsonObj = batchJsonElement.getAsJsonObject();
      Batch batch = new Batch();

      batch.setBatchId(jsonObj.getAsJsonPrimitive("batchId").getAsInt());
      batch.setBatchTime(jsonObj.getAsJsonPrimitive("batchTime").getAsString());
      batch.setStatus(batchStatus);
      batch.setBatchDuration(jsonObj.getAsJsonPrimitive("batchDuration").getAsInt());
      batch.setInputSize(jsonObj.getAsJsonPrimitive("inputSize").getAsInt());
      if (batchStatus == BatchStatus.COMPLETED) {
        batch.setSchedulingDelay(jsonObj.getAsJsonPrimitive("schedulingDelay").getAsInt());
        batch.setProcessingTime(jsonObj.getAsJsonPrimitive("processingTime").getAsInt());
        batch.setTotalDelay(jsonObj.getAsJsonPrimitive("totalDelay").getAsInt());
      }
      if (batchStatus == BatchStatus.PROCESSING) {
        batch.setSchedulingDelay(jsonObj.getAsJsonPrimitive("schedulingDelay").getAsInt());
      }
      batch.setNumActiveOutputOps(jsonObj.getAsJsonPrimitive("numActiveOutputOps").getAsInt());
      batch.setNumCompletedOutputOps(
          jsonObj.getAsJsonPrimitive("numCompletedOutputOps").getAsInt());
      batch.setNumFailedOutputOps(jsonObj.getAsJsonPrimitive("numFailedOutputOps").getAsInt());
      batch.setNumTotalOutputOps(jsonObj.getAsJsonPrimitive("numTotalOutputOps").getAsInt());

      batches.add(batch);
    }

    return batches;
  }

  /**
   * Get batches of the specified name and status.
   *
   * @param appName  application name
   * @return
   * @throws Exception
   */
  public List<Batch> getAllBatches(String appName) throws Exception {
    ApplicationReport applicationReport = getApplicationReport(appName);
    Preconditions.checkState(
        Objects.nonNull(applicationReport), "Application %s is not running on yarn", appName);

    String response =
        HttpClientUtil.doGet(
            String.format(
                BATCHES_REST_API,
                applicationReport.getTrackingUrl(),
                applicationReport.getApplicationId().toString()));

    JsonArray batchJsonObjs = parser.parse(response).getAsJsonArray();

    List<Batch> batches = new ArrayList<>();

    for (JsonElement batchJsonElement : batchJsonObjs) {
      JsonObject jsonObj = batchJsonElement.getAsJsonObject();
      Batch batch = new Batch();

      batch.setBatchId(jsonObj.getAsJsonPrimitive("batchId").getAsInt());
      batch.setBatchTime(jsonObj.getAsJsonPrimitive("batchTime").getAsString());
      String status = jsonObj.getAsJsonPrimitive("status").getAsString();
      BatchStatus batchStatus;
      switch (status) {
        case QUEUED:
          batchStatus = BatchStatus.QUEUED;
          break;
        case PROCESSING:
          batchStatus = BatchStatus.PROCESSING;
          break;
        default:
          batchStatus = BatchStatus.COMPLETED;
          break;
      }
      batch.setStatus(batchStatus);
      batch.setBatchDuration(jsonObj.getAsJsonPrimitive("batchDuration").getAsInt());
      batch.setInputSize(jsonObj.getAsJsonPrimitive("inputSize").getAsInt());
      if (batchStatus == BatchStatus.COMPLETED) {
        batch.setSchedulingDelay(jsonObj.getAsJsonPrimitive("schedulingDelay").getAsInt());
        batch.setProcessingTime(jsonObj.getAsJsonPrimitive("processingTime").getAsInt());
        batch.setTotalDelay(jsonObj.getAsJsonPrimitive("totalDelay").getAsInt());
      }
      if (batchStatus == BatchStatus.PROCESSING) {
        batch.setSchedulingDelay(jsonObj.getAsJsonPrimitive("schedulingDelay").getAsInt());
      }
      batch.setNumActiveOutputOps(jsonObj.getAsJsonPrimitive("numActiveOutputOps").getAsInt());
      batch.setNumCompletedOutputOps(
          jsonObj.getAsJsonPrimitive("numCompletedOutputOps").getAsInt());
      batch.setNumFailedOutputOps(jsonObj.getAsJsonPrimitive("numFailedOutputOps").getAsInt());
      batch.setNumTotalOutputOps(jsonObj.getAsJsonPrimitive("numTotalOutputOps").getAsInt());

      batches.add(batch);
    }

    return batches;
  }

  /**
   * Submit the application of the specified name.
   *
   * @param appName application name
   */
  public void submit(String appName) {
    // TODO
  }

  /**
   * Kill the application of the specified name.
   *
   * @param appName application name
   */
  public void kill(String appName) {
    EnumSet<YarnApplicationState> aliveStates = getAliveStates();
    ApplicationReport applicationReport = getApplication(aliveStates, appName);
    if (applicationReport == null) {
      return;
    }
    try {
      yarnClient.killApplication(applicationReport.getApplicationId());
      LOGGER.info("Application {} killed", applicationReport.getApplicationId().toString());
    } catch (Exception e) {
      ExceptionUtils.getFullStackTrace(e);
    }
  }

  @Override
  public void close() {
    if (Objects.nonNull(yarnClient)) {
      yarnClient.stop();
    }
  }

}
