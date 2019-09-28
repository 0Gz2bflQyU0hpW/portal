package com.weibo.dip.platform.client;

import com.weibo.dip.platform.model.Basic;
import com.weibo.dip.platform.model.StateEnum;
import com.weibo.dip.platform.model.Streaming;
import com.weibo.dip.web.model.realtime.StreamingInfo;
import com.weibo.dip.platform.mysql.BasicdbUtil;
import com.weibo.dip.platform.mysql.StreamingInfoUtil;
import com.weibo.dip.platform.mysql.StreamingdbUtil;
import com.weibo.dip.platform.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Client {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

  private static final int UPDATE_DB_SUCCESS = 1;
  private BasicdbUtil basicdbUtil = new BasicdbUtil();
  private StreamingdbUtil streamingdbUtil = new StreamingdbUtil();
  private StreamingInfoUtil streamingInfoUtil = new StreamingInfoUtil();

  /**
   * start appplicetion only change the state.
   *
   * @param appName application name
   * @return success of not
   */
  public int startApplication(String appName, String creator) {
    Basic basic = basicdbUtil.getByAppName(appName);
    if (basic == null) {
      LOGGER.error("couldn't found {} basic info.", appName);
      return Constants.APP_NO_FOUND;
    }
    int result = streamingdbUtil.insertNotExists(appName, creator);
    if (result < UPDATE_DB_SUCCESS) {
      LOGGER.error("start application {} error, {} maybe exist", appName, appName);
      return Constants.FAILED;
    }
    LOGGER.info("insert application {} wait start", appName);
    return result;
  }

  /**
   * kill appplicetion only change the state.
   *
   * @param appName application name
   * @return success of not
   */
  public int killApplication(String appName) {
    Streaming streaming = streamingdbUtil.selectByAppName(appName);
    if (streaming == null) {
      LOGGER.error("couldn't found {} running", appName);
      return Constants.FAILED;
    }
    int result =
        streamingdbUtil.updateState(
            streaming.getId(), StateEnum.WAITKILL.getNumber(), StateEnum.getKillPreviousStates());
    if (result != UPDATE_DB_SUCCESS) {
      LOGGER.error("fail to update {} state WAITKILL", appName);
      return Constants.FAILED;
    }
    LOGGER.info("update application {} wait kill", appName);
    return Constants.SUCCESS;
  }

  /**
   * restart.
   *
   * @param appName application name
   * @return success or failed
   */
  public int restartApplication(String appName) {
    Streaming streaming = streamingdbUtil.selectByAppName(appName);
    if (streaming == null) {
      LOGGER.error("couldn't found {} running", appName);
      return Constants.FAILED;
    }
    int result =
        streamingdbUtil.updateState(
            streaming.getId(),
            StateEnum.WAITRESTART.getNumber(),
            StateEnum.getKillPreviousStates());
    if (result != UPDATE_DB_SUCCESS) {
      LOGGER.error("fail to update {} state WAITRESTART", appName);
      return Constants.FAILED;
    }
    LOGGER.info("update application {} wait restart", appName);
    return Constants.SUCCESS;
  }

  /**
   * list all application.
   *
   * @return list
   */
  public List<StreamingInfo> listAllApplication() {
    return streamingInfoUtil.list();
  }

  /**
   * insert info.
   *
   * @param streamingInfo info
   */
  public int insert(StreamingInfo streamingInfo) {
    return streamingInfoUtil.insert(streamingInfo);
  }

  /**
   * delete.
   *
   * @param name name
   */
  public int delete(String name) {
    return streamingInfoUtil.delete(name);
  }

  /**
   * update.
   *
   * @param streamingInfo info
   */
  public int update(StreamingInfo streamingInfo) {
    return streamingInfoUtil.update(streamingInfo);
  }

  /**
   * find by name.
   *
   * @param name name
   */
  public StreamingInfo findByName(String name) {
    return streamingInfoUtil.findByName(name);
  }

  /**
   * exit or not .
   *
   * @param name name
   */
  public boolean isExit(String name) {
    return streamingInfoUtil.isExit(name);
  }
}
