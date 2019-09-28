package com.weibo.dip.streaming;

import com.weibo.dip.streaming.bean.Executor;
import com.weibo.dip.streaming.bean.Receiver;
import com.weibo.dip.streaming.bean.Statistic;
import com.weibo.dip.streaming.bean.application.Application;
import com.weibo.dip.streaming.bean.application.ApplicationStatus;
import com.weibo.dip.streaming.bean.batch.Batch;
import com.weibo.dip.streaming.bean.batch.BatchStatus;
import com.weibo.dip.streaming.bean.job.Job;
import com.weibo.dip.streaming.bean.job.JobStatus;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming rest client tester.
 *
 * @author yurun
 */
public class StreamingRestClientTester {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingRestClientTester.class);

  private static final StreamingClient STREAMING_REST_CLIENT = new StreamingClient();

  private static final String APP_NAME = "CdnPicClientStreamingV1";

  public static void testGetApplications() throws Exception {
    List<Application> applications =
        STREAMING_REST_CLIENT.getApplications(APP_NAME, ApplicationStatus.RUNNING);

    for (Application application : applications) {
      LOGGER.info(application.toString());
    }
  }

  public static void testGetJobs() throws Exception {
    List<Job> jobs = STREAMING_REST_CLIENT.getJobs(APP_NAME, JobStatus.RUNNING);

    for (Job job : jobs) {
      LOGGER.info(job.toString());
    }
  }

  public static void testGetExecutors() throws Exception {
    List<Executor> executors = STREAMING_REST_CLIENT.getExecutors(APP_NAME);

    for (Executor executor : executors) {
      LOGGER.info(executor.toString());
    }
  }

  public static void testAllGetExecutors() throws Exception {
    List<Executor> executors = STREAMING_REST_CLIENT.getAllExecutors(APP_NAME);

    for (Executor executor : executors) {
      LOGGER.info(executor.toString());
    }
  }

  public static void testGetStatistic() throws Exception {
    Statistic statistic = STREAMING_REST_CLIENT.getStatistic(APP_NAME);

    LOGGER.info(statistic.toString());
  }

  public static void testYarnClient() throws Exception {
    Configuration conf = new Configuration();

    YarnClient yarnClient = YarnClient.createYarnClient();

    yarnClient.init(conf);

    yarnClient.start();

    List<ApplicationReport> applicationReports =
        yarnClient.getApplications(
            Collections.singleton("SPARK"), EnumSet.of(YarnApplicationState.RUNNING));

    yarnClient.stop();

    for (ApplicationReport applicationReport : applicationReports) {
      LOGGER.info(applicationReport.getTrackingUrl());
    }

    LOGGER.info("applications: {}", applicationReports.size());
  }

  public static void testGetReceivers() throws Exception {
    List<Receiver> receivers = STREAMING_REST_CLIENT.getReceivers(APP_NAME);

    for (Receiver receiver : receivers) {
      LOGGER.info(receiver.toString());
    }
  }

  public static void testGetBatchs() throws Exception {
    List<Batch> batches = STREAMING_REST_CLIENT.getBatchs(APP_NAME, BatchStatus.COMPLETED);

    for (Batch batch : batches) {
      LOGGER.info(batch.toString());
    }
  }

  public static void main(String[] args) throws Exception {
    testGetBatchs();
  }
}
