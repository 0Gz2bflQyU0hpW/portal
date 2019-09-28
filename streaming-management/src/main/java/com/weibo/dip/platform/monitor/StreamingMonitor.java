package com.weibo.dip.platform.monitor;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.commons.Symbols;
import com.weibo.dip.data.platform.commons.util.WatchAlert;
import com.weibo.dip.platform.model.Alert;
import com.weibo.dip.platform.model.StateEnum;
import com.weibo.dip.platform.model.Streaming;
import com.weibo.dip.platform.mysql.AlertdbUtil;
import com.weibo.dip.platform.mysql.StreamingdbUtil;
import com.weibo.dip.platform.service.StreamingService;
import com.weibo.dip.platform.util.Constants;
import com.weibo.dip.streaming.StreamingClient;
import com.weibo.dip.streaming.bean.Executor;
import com.weibo.dip.streaming.bean.Statistic;
import com.weibo.dip.streaming.bean.batch.Batch;
import com.weibo.dip.streaming.bean.batch.BatchStatus;
import com.weibo.dip.streaming.bean.job.Job;
import com.weibo.dip.streaming.bean.job.JobStatus;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingMonitor {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingMonitor.class);

  private static final int ALERT_BATCHES = 10;
  private static final int DO_ALERT = 1;
  private static final int START_TIMEOUT = 180;
  private static final String FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'";
  private StreamingClient streamingClient = new StreamingClient();
  private AlertdbUtil alertdbUtil = new AlertdbUtil();
  private StreamingdbUtil streamingdbUtil = new StreamingdbUtil();
  private StreamingService streamingService = new StreamingService();

  public static void main(String[] args) throws Exception {
    LOGGER.info("start to do alert.");
    new StreamingMonitor().doAlert();
  }

  private List<Alert> getAlertInfos() {
    return alertdbUtil.getAlertInfos();
  }

  private int getMinute() {
    Calendar now = Calendar.getInstance();
    return now.get(Calendar.MINUTE);
  }

  /**
   * do alert.
   *
   * @throws Exception .
   */
  private void doAlert() throws Exception {
    List<ApplicationReport> applicationReports = streamingClient
        .getApplications(streamingClient.getAliveStates());
    Preconditions.checkState(applicationReports.size() > 0, "can't find any application on yarn");

    int minute = getMinute();
    List<Streaming> streamings = streamingdbUtil.selectAll();
    //alertStartingTimeout(streamings, applicationReports, minute);

    List<Alert> alerts = getAlertInfos();
    for (Alert alert : alerts) {
      if (minute % alert.getAlertInterval() == 0) {
        String appName = alert.getAppName();
        LOGGER.info("do alert application: {}", appName);
        int result = alertAliveAndRepetitive(alert, applicationReports);
        if (result == Constants.SKIP_MONITOR) {
          continue;
        }

        Statistic statistic = streamingClient.getStatistic(appName);
        List<Batch> completedBatches = streamingClient.getBatchs(appName, BatchStatus.COMPLETED);
        int batchDuration = statistic.getBatchDuration();

        StringBuilder alertMgs = new StringBuilder();
        alertMgs
            .append(alertReceiveRecords(alert, statistic))
            .append(alertExecutorsAlive(alert))
            .append(alertReceiversAlive(alert, statistic))
            .append(alertAccumulation(alert, statistic))
            .append(alertActiveBatches(alert, statistic, batchDuration))
            .append(alertActiveBatchesProcessingTimeout(alert, batchDuration))
            .append(alertSchedulerDelay(alert, batchDuration, completedBatches))
            .append(alertProcessingDelay(alert, batchDuration, completedBatches));
        if (alertMgs.toString().length() > 0) {
          sendToGroupAlert(appName, Constants.WARN + Symbols.NEWLINE + alertMgs.toString(), alertMgs.toString(),
              new String[]{alert.getAlertGroup()});
        }
      }
    }
  }

  // alert repetitive applications, alert application not running
  private int alertAliveAndRepetitive(Alert alert, List<ApplicationReport> applicationReports) {
    String appName = alert.getAppName();
    int totalNum = 0;
    for (ApplicationReport application : applicationReports) {
      if (appName.equals(application.getName())) {
        totalNum++;
      }
    }
    if (DO_ALERT == alert.getAlertRepetitive() && totalNum > 1) {
      String content = String.format("multiple %s were found!" + Symbols.NEWLINE, appName);
      sendToGroupAlert(appName, Constants.ERROR + Symbols.NEWLINE + content, content,
          new String[]{alert.getAlertRepetitiveGroup()});
    }
    if (DO_ALERT == alert.getAlertAlive() && totalNum == 0) {
      String content = String.format("%s is not running, restart!", appName);
      sendToGroupAlert(appName, Constants.ERROR + Symbols.NEWLINE + content, content,
          new String[]{alert.getAlertGroup()});
      //streamingService.restart(streaming);
      LOGGER.info("{} is not alive, restart!", appName);
      restart(alert.getRestartCmd());
    }
    if (totalNum != 1) {
      return Constants.SKIP_MONITOR;
    }
    LOGGER.info("{} is alive.", appName);
    return Constants.CONTINUE_MONITOR;
  }

  // alert receive records suddently reduce
  private StringBuilder alertReceiveRecords(Alert alert, Statistic statistic) throws Exception {
    StringBuilder stringBuilder = new StringBuilder();
    String appName = alert.getAppName();
    List<Batch> allBatches = streamingClient.getAllBatches(appName);
    if (DO_ALERT == alert.getAlertReceive() && allBatches.size() > ALERT_BATCHES) {
      int totalNum = 0;
      for (int index = 0; index < ALERT_BATCHES; index++) {
        totalNum += allBatches.get(index).getInputSize();
      }
      if (totalNum / ALERT_BATCHES
          < statistic.getAvgInputRate() * alert.getAlertReceiveThreshold()) {
        stringBuilder.append(
            String.format(
                "recently %s batches' avg input size is %s, less than %s * AvgInputRate(%s). \n",
                ALERT_BATCHES, totalNum / ALERT_BATCHES, alert.getAlertReceiveThreshold(), (int)statistic.getAvgInputRate()));
      }
    }
    return stringBuilder;
  }

  // alert long time no exist active batch
  private StringBuilder alertActiveBatches(Alert alert, Statistic statistic, int batchDuration)
      throws Exception {
    StringBuilder stringBuilder = new StringBuilder();
    String appName = alert.getAppName();
    if (DO_ALERT != alert.getAlertActive()) {
      return stringBuilder;
    }
    if (statistic.getNumActiveBatches() == 0) {
      if (statistic.getNumTotalCompletedBatches() == 0) {
        stringBuilder.append("no active and completed batch." + Symbols.NEWLINE);
      } else {
        Batch batch = streamingClient.getBatchs(appName, BatchStatus.COMPLETED).get(0);
        Date batchTime = getLocalTime(batch.getBatchTime());
        long distancs = getDistance(batchTime);
        if (distancs >= batchDuration * alert.getAlertActiveThreshold()) {
          stringBuilder.append(
              String.format(
                  "%s seconds no active batch, threshold is %s * %s." + Symbols.NEWLINE,
                  distancs, alert.getAlertActiveThreshold(), batchDuration));
        }
      }
    } else {
      List<Job> jobs = streamingClient.getJobs(alert.getAppName(), JobStatus.SUCCEEDED);
      if (jobs != null && !jobs.isEmpty()) {
        Job job = jobs.get(0);
        long jobDistancs = getDistance(getLocalTime(job.getCompletionTime()));
        if (statistic.getNumTotalCompletedBatches() == 0) {
          if (jobDistancs >= batchDuration * alert.getAlertActiveThreshold()) {
            stringBuilder.append(
                String.format(
                    "%s seconds no active batch, threshold is %s * %s." + Symbols.NEWLINE,
                    jobDistancs, alert.getAlertActiveThreshold(), batchDuration));
          }
        } else {
          Batch batch = streamingClient.getBatchs(appName, BatchStatus.COMPLETED).get(0);
          long batchDistancs = getDistance(getLocalTime(batch.getBatchTime()));
          if (jobDistancs + batchDistancs >= batchDuration * alert.getAlertActiveThreshold()) {
            stringBuilder.append(
                String.format(
                    "%s seconds no active batch, threshold is %s * %s ." + Symbols.NEWLINE,
                    jobDistancs + batchDistancs, alert.getAlertActiveThreshold(), batchDuration));
          }
        }
      }
    }
    return stringBuilder;
  }

  // alert too many batches accumulation
  private StringBuilder alertAccumulation(Alert alert, Statistic statistic) {
    StringBuilder stringBuilder = new StringBuilder();
    if (DO_ALERT == alert.getAlertAccumulation()
        && statistic.getNumActiveBatches() >= alert.getAlertAccumulationThreshold()) {
      stringBuilder.append(
          String.format(
              "%s batches is accumulation, threshold is %s." + Symbols.NEWLINE,
              statistic.getNumActiveBatches(), alert.getAlertAccumulationThreshold()));
    }
    return stringBuilder;
  }

  // alert last compete batch sheduler delay
  private StringBuilder alertSchedulerDelay(
      Alert alert, int batchDuration, List<Batch> completedBatches) {
    StringBuilder stringBuilder = new StringBuilder();
    if (DO_ALERT == alert.getAlertSchedulingDelay() && completedBatches.size() > 0) {
      Batch batch = completedBatches.get(0);
      if (batch.getSchedulingDelay() >= batchDuration * alert.getAlertSchedulingDelayThreshold()) {
        stringBuilder.append(
            String.format(
                "last competed batch's Scheduling Delay is %s s, "
                    + " threshold is %s s." + Symbols.NEWLINE,
                batch.getSchedulingDelay() / 1000, batchDuration * alert
                    .getAlertSchedulingDelayThreshold() / 1000));
      }
    }
    return stringBuilder;
  }

  // alert last compete batch processing delay
  private StringBuilder alertProcessingDelay(
      Alert alert, int batchDuration, List<Batch> completedBatches) {
    StringBuilder stringBuilder = new StringBuilder();
    if (DO_ALERT == alert.getAlertProcessing() && completedBatches.size() > 0) {
      Batch batch = completedBatches.get(0);
      if (batch.getProcessingTime() >= batchDuration * alert.getAlertProcessingThreshold()) {
        stringBuilder.append(
            String.format(
                "last competed batch's Processing Time is %s s, threshold is %s s." + Symbols.NEWLINE,
                batch.getProcessingTime() / 1000, batchDuration * alert
                    .getAlertProcessingThreshold() / 1000));
      }
    }
    return stringBuilder;
  }

  // alert one or more active batches processing time too long
  private StringBuilder alertActiveBatchesProcessingTimeout(Alert alert, int batchDuration)
      throws Exception {
    StringBuilder stringBuilder = new StringBuilder();
    String appName = alert.getAppName();
    List<Batch> processingBatches = streamingClient.getBatchs(appName, BatchStatus.PROCESSING);
    if (DO_ALERT == alert.getAlertActiveProcessing() && processingBatches.size() > 0) {
      int totalNum = 0;
      for (Batch batch : processingBatches) {
        long processingTime = getDistance(getLocalTime(batch.getBatchTime()));
        if (processingTime > batchDuration * alert.getAlertActiveProcessingTimeThreshold()) {
          totalNum++;
        }
      }
      if (totalNum >= alert.getAlertActiveProcessingNumThreshold()) {
        stringBuilder.append(
            String.format(
                "%s active batches is processing delay,"
                    + " time_threshold is %s,"
                    + " num_threshold is %s." + Symbols.NEWLINE,
                totalNum,
                alert.getAlertActiveProcessingTimeThreshold(),
                alert.getAlertActiveProcessingNumThreshold()));
      }
    }
    return stringBuilder;
  }

  // alert receivers alive
  private StringBuilder alertReceiversAlive(Alert alert, Statistic statistic) {
    StringBuilder stringBuilder = new StringBuilder();
    if (DO_ALERT == alert.getAlertInactiveReceivers()) {
      int inactiveReceivers = statistic.getNumInactiveReceivers();
      if (inactiveReceivers >= alert.getAlertInactiveReceiversThreshold()) {
        stringBuilder.append(
            String.format(
                "%s receivers is inactive, threshold is %s." + Symbols.NEWLINE,
                inactiveReceivers, alert.getAlertInactiveReceiversThreshold()));
      }
    }
    return stringBuilder;
  }

  // alert executor alive
  private StringBuilder alertExecutorsAlive(Alert alert) throws Exception {
    StringBuilder stringBuilder = new StringBuilder();
    String appName = alert.getAppName();
    if (DO_ALERT == alert.getAlertInactiveExecutors()) {
      List<Executor> executors = streamingClient.getAllExecutors(appName);
      if (executors.size() > 0) {
        int inactiveExecutors = 0;
        for (Executor executor : executors) {
          if (!executor.isActive()) {
            inactiveExecutors++;
          }
        }
        if (inactiveExecutors >= alert.getAlertInactiveExecutorsThreshold()) {
          stringBuilder.append(
              String.format(
                  "%s executors is inactive, threshold is %s." + Symbols.NEWLINE,
                  inactiveExecutors, alert.getAlertInactiveExecutorsThreshold()));
        }
      }
    }
    return stringBuilder;
  }

  private void alertStartingTimeout(List<Streaming> streamings,
      List<ApplicationReport> applicationReports, int minute) {
    HashMap<String, YarnApplicationState> map = new HashMap<>();
    if (applicationReports != null) {
      for (ApplicationReport application : applicationReports) {
        map.put(application.getName(), application.getYarnApplicationState());
      }
    }
    for (Streaming streaming : streamings) {
      if (StateEnum.YARNSUCCESS.getNumber() == streaming.getState()) {
        continue;
      }
      long distance = getDistance(streaming.getLastUpdateTime());
      if (distance < START_TIMEOUT) {
        continue;
      }
      String appName = streaming.getAppName();
      if (map.containsKey(appName)) {
        if (YarnApplicationState.RUNNING == map.get(appName)) {
          streamingdbUtil.updateState(streaming.getId(), StateEnum.YARNSUCCESS.getNumber(),
              StateEnum.getYarnPreviousStates());
          alertdbUtil.updateStateByAppName(Constants.START_MONITOR, appName);
        } else {
          String content = String
              .format("application %s is on yarn more than %s seconds, but not running.", appName,
                  START_TIMEOUT);
          sendToUserAlert(appName, Constants.WARN, content, new String[]{streaming.getCreator()});
        }
      } else {
        String content = String
            .format("application %s couldn't find on yarn more than %s seconds, restart.", appName,
                START_TIMEOUT);
        sendToUserAlert(appName, Constants.WARN, content, new String[]{streaming.getCreator()});
        streamingService.restart(streaming);
      }
    }
  }

  private void restart(String cmd) {
    Runtime run = Runtime.getRuntime();
    try {
      Process process = run.exec(cmd);
      process.waitFor();
      process.destroy();
    } catch (Exception e) {
      ExceptionUtils.getFullStackTrace(e);
    }
  }

  private long getDistance(Date lastUpdateTime) {
    long now = new Date().getTime();
    long last = lastUpdateTime.getTime();
    if (now > last) {
      return (int) ((now - last) / 1000);
    }
    return -1L;
  }

  private Date getLocalTime(String time) throws ParseException {
    SimpleDateFormat sd = new SimpleDateFormat(FORMAT);
    Date date = sd.parse(time);
    long timestamp = date.getTime() + 8 * 60 * 60 * 1000;
    return new Date(timestamp);
  }

  private void sendToGroupAlert(String subService, String subject, String content,
      String[] groups) {
    try {
      WatchAlert.sendAlarmToGroups(Constants.SERVICE, subService, subject, content, groups);
    } catch (Exception e) {
      LOGGER.error("send %S alert message error", subService);
    }
  }

  private void sendToUserAlert(String subService, String subject, String content, String[] groups) {
    try {
      WatchAlert.sendAlarmToUsers(Constants.SERVICE, subService, subject, content, groups);
    } catch (Exception e) {
      LOGGER.error("send %S alert message error", subService);
    }
  }

}
