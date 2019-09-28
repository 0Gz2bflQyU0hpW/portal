package com.weibo.dip.platform.service;

import com.github.dockerjava.api.command.CreateContainerResponse;
import com.weibo.dip.data.platform.commons.util.WatchAlert;
import com.weibo.dip.platform.docker.DockerManager;
import com.weibo.dip.platform.model.Basic;
import com.weibo.dip.platform.model.StateEnum;
import com.weibo.dip.platform.model.Streaming;
import com.weibo.dip.platform.mysql.AlertdbUtil;
import com.weibo.dip.platform.mysql.BasicdbUtil;
import com.weibo.dip.platform.mysql.StreamingdbUtil;
import com.weibo.dip.platform.util.Constants;
import com.weibo.dip.platform.yarn.YarnResourceUtil;
import com.weibo.dip.streaming.StreamingClient;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingService.class);

  private static final int UPDATE_DB_SUCCESS = 1;
  private static final int RETYR_TIMES = 40;
  private static final int SLEEP_SECONDS = 3;
  private static final boolean CONTAINER_CREATED = true;
  private static final String ALERT_GROUP = "DIP_STREAMING_TEST";

  private BasicdbUtil basicdbUtil = new BasicdbUtil();
  private AlertdbUtil alertdbUtil = new AlertdbUtil();
  private StreamingdbUtil streamingdbUtil = new StreamingdbUtil();
  private DockerManager dockerManager = new DockerManager();
  private YarnResourceUtil yarnResourceUtil = new YarnResourceUtil();
  private StreamingClient streamingClient = new StreamingClient();

  /**
   * start .
   *
   * @param streaming information
   */
  public void start(Streaming streaming) {
    String appName = streaming.getAppName();
    Basic basic = basicdbUtil.getByAppName(appName);

    int queue = yarnResourceUtil.getCapacity(basic.getQueue());
    if (queue < 1) {
      String content = String.format("source of %s is not enough", appName);
      stopStarting(content, content, new String[]{streaming.getCreator(), ALERT_GROUP}, streaming,
          StateEnum.STARTPOOLFAILD.getNumber(), !CONTAINER_CREATED);
      return;
    }

    boolean pull = dockerManager.pullImage(basic.getImageName(), basic.getImageTag());
    if (!pull) {
      String content = String.format("fail to pull image %s", appName);
      stopStarting(content, content, new String[]{streaming.getCreator(), ALERT_GROUP}, streaming,
          StateEnum.STARTPOOLFAILD.getNumber(), !CONTAINER_CREATED);
      return;
    }

    CreateContainerResponse container =
        dockerManager.createContainer(basic.getImageName(), basic.getImageTag(), appName);
    if (container == null) {
      String content = String.format("fail to create Container %s", appName);
      stopStarting(content, content, new String[]{streaming.getCreator(), ALERT_GROUP}, streaming,
          StateEnum.STARTPOOLFAILD.getNumber(), !CONTAINER_CREATED);
      return;
    }
    boolean isContinue = isContinue(streaming, StateEnum.STARTPOOL);
    if (!isContinue) {
      return;
    }

    int exitCode = dockerManager.startContainer(container, appName);
    if (exitCode != 0) {
      String subject = String
          .format("docker container exitCode != 0, application %s start failed.", appName);
      String content = dockerManager.getLog(container.getId());
      stopStarting(subject, content, new String[]{streaming.getCreator(), ALERT_GROUP}, streaming,
          StateEnum.STARTPOOLFAILD.getNumber(), CONTAINER_CREATED);
      return;
    }
    int result = streamingdbUtil.updateState(streaming.getId(), StateEnum.SUBMITSUCCESS.getNumber(),
        Arrays.asList(StateEnum.STARTPOOL.getNumber()));
    if (result != UPDATE_DB_SUCCESS) {
      LOGGER.error("fail to update {} state SUBMIT_SUCCESS, maybe killing", appName);
      return;
    }

    int count = RETYR_TIMES;
    while (count > 0) {
      ApplicationReport application =
          streamingClient.getApplication(streamingClient.getAliveStates(), appName);
      if (application != null) {
        YarnApplicationState yarnApplicationState = application.getYarnApplicationState();

        int yarnState = StateEnum.SUBMITSUCCESS.getNumber();
        switch (yarnApplicationState) {
          case SUBMITTED:
            LOGGER.info("application {} state is SUBMITTED", appName);
            count--;
            yarnState = StateEnum.YARNGET.getNumber();
            break;
          case ACCEPTED:
            LOGGER.info("application {} state is ACCEPTED", appName);
            count--;
            yarnState = StateEnum.YARNGET.getNumber();
            break;
          case RUNNING:
            LOGGER.info("application {} state is RUNNING", appName);
            count = 0;
            yarnState = StateEnum.YARNSUCCESS.getNumber();
            alertdbUtil.updateStateByAppName(Constants.START_MONITOR, appName);
            break;
          default:
            LOGGER.info("couldn't find application {} state on yarn", appName);
            count--;
        }
        result =
            streamingdbUtil.updateState(
                streaming.getId(),
                yarnState,
                StateEnum.getYarnPreviousStates(),
                application.getApplicationId().toString());
        if (result == 0) {
          LOGGER.error("fail to update {} state, maybe killing", appName);
          return;
        }
      }
      try {
        TimeUnit.SECONDS.sleep(SLEEP_SECONDS);
      } catch (InterruptedException e) {
        ExceptionUtils.getFullStackTrace(e);
      }
    }
  }

  /**
   * kill.
   *
   * @param streaming running application message
   */
  public void kill(Streaming streaming) {
    String appName = streaming.getAppName();
    stopMonitor(appName);
    LOGGER.info("stop application {} monitor.", appName);
    removeStreaming(streaming, StateEnum.KILLED.getNumber(), CONTAINER_CREATED);
    LOGGER.info("kill application {} success.", appName);
  }

  /**
   * restart.
   */
  public void restart(Streaming streaming) {
    String appName = streaming.getAppName();
    removeStreaming(streaming, StateEnum.KILLED.getNumber(), CONTAINER_CREATED);
    LOGGER.info("kill application {} success.", appName);
    streamingdbUtil.insertNotExists(appName, streaming.getCreator());
    LOGGER.info("insert application {} wait start", appName);
  }

  private void removeStreaming(Streaming streaming, int failState, boolean isContainerCreated) {
    streaming.setState(failState);
    streamingdbUtil.insertHistory(streaming);
    streamingdbUtil.deleteById(streaming.getId());
    String appName = streaming.getAppName();
    LOGGER.info("remove application {} from database.", appName);
    if (isContainerCreated) {
      dockerManager.removeContainer(appName);
      streamingClient.kill(appName);
      LOGGER.info("remove application {} from yarn.", appName);
    }
  }

  private boolean isContinue(Streaming streaming, StateEnum expectedState) {
    boolean flag = true;
    if (streamingdbUtil.selectById(streaming.getId()) == null
        || expectedState.getNumber()
        != streamingdbUtil.selectById(streaming.getId()).getState()) {
      LOGGER.error("couldn't find Streaming {} from mysql, maybe killed.", streaming.getAppName());
      flag = false;
    }
    return flag;
  }

  private void stopMonitor(String appName) {
    alertdbUtil.updateStateByAppName(Constants.SKIP_MONITOR, appName);
  }

  private void stopStarting(String subject, String content, String[] tos, Streaming streaming,
      int failState, boolean bool) {
    LOGGER.error(content);
    removeStreaming(streaming, failState, bool);
    alert(subject, content, tos);
  }

  private void alert(String subject, String content, String[] tos) {
    try {
      WatchAlert.alert(Constants.SERVICE, Constants.ERROR, subject, content, tos);
    } catch (Exception e) {
      ExceptionUtils.getFullStackTrace(e);
    }
  }

}
