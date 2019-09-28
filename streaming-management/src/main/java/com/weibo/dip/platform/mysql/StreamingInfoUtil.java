package com.weibo.dip.platform.mysql;

import com.weibo.dip.platform.model.Alert;
import com.weibo.dip.platform.model.Basic;
import com.weibo.dip.web.model.realtime.StreamingInfo;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/** Created by haisen on 2018/8/8. */
public class StreamingInfoUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingdbUtil.class);

  private static final DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static BasicdbUtil basicdbUtil = new BasicdbUtil();

  private static AlertdbUtil alertdbUtil = new AlertdbUtil();

  private static StreamingdbUtil streamingdbUtil = new StreamingdbUtil();

  private static final String LIST_ALL_SQL =
      "select basic.*,"
          + "info.create_time,"
          + "info.last_update_time,"
          + "info.app_id,"
          + "info.state,"
          + "info.creator,"
          + "info.killer,"
          + "alert.alert_accumulation,"
          + "alert.alert_accumulation_threshold,"
          + "alert.alert_active,"
          + "alert.alert_active_processing,"
          + "alert.alert_active_processing_num_threshold,"
          + "alert.alert_active_processing_time_threshold,"
          + "alert.alert_active_threshold,"
          + "alert.alert_alive,"
          + "alert.alert_error,"
          + "alert.alert_error_threshold,"
          + "alert.alert_group,"
          + "alert.alert_inactive_executors,"
          + "alert.alert_inactive_executors_threshold,"
          + "alert.alert_inactive_receivers,"
          + "alert.alert_inactive_receivers_threshold,"
          + "alert.alert_interval,"
          + "alert.alert_processing,"
          + "alert.alert_processing_threshold,"
          + "alert.alert_receive,"
          + "alert.alert_receive_threshold,"
          + "alert.alert_repetitive,"
          + "alert.alert_repetitive_group,"
          + "alert.alert_scheduling_delay,"
          + "alert.alert_scheduling_delay_threshold,"
          + "alert.collection,"
          + "alert.state as alert_state "
          + "from streaming_basic basic "
          + "left join streaming_appinfo info on basic.app_name = info.app_name "
          + "left join streaming_alert alert on basic.app_name = alert.app_name";

  private static final String FIND_BY_NAME =
      "select basic.*,"
          + "info.create_time,"
          + "info.last_update_time,"
          + "info.app_id,"
          + "info.state,"
          + "info.creator,"
          + "info.killer,"
          + "alert.alert_accumulation,"
          + "alert.alert_accumulation_threshold,"
          + "alert.alert_active,"
          + "alert.alert_active_processing,"
          + "alert.alert_active_processing_num_threshold,"
          + "alert.alert_active_processing_time_threshold,"
          + "alert.alert_active_threshold,"
          + "alert.alert_alive,"
          + "alert.alert_error,"
          + "alert.alert_error_threshold,"
          + "alert.alert_group,"
          + "alert.alert_inactive_executors,"
          + "alert.alert_inactive_executors_threshold,"
          + "alert.alert_inactive_receivers,"
          + "alert.alert_inactive_receivers_threshold,"
          + "alert.alert_interval,"
          + "alert.alert_processing,"
          + "alert.alert_processing_threshold,"
          + "alert.alert_receive,"
          + "alert.alert_receive_threshold,"
          + "alert.alert_repetitive,"
          + "alert.alert_repetitive_group,"
          + "alert.alert_scheduling_delay,"
          + "alert.alert_scheduling_delay_threshold,"
          + "alert.collection,"
          + "alert.state as alert_state "
          + "from streaming_basic basic "
          + "left join streaming_appinfo info on basic.app_name = info.app_name "
          + "left join streaming_alert alert on basic.app_name = alert.app_name "
          + "where basic.app_name = ?";

  private static final String DELETE_BY_NAME =
      "delete streaming_basic,streaming_alert "
          + "from streaming_basic left join streaming_alert "
          + "on streaming_basic.app_name=streaming_alert.app_name "
          + "where streaming_basic.app_name=? and "
          + "not exists (SELECT * FROM streaming_appinfo WHERE app_name =?)";
  /**
   * list all.
   *
   * @return list
   */
  public List<StreamingInfo> list() {
    List<StreamingInfo> list = new ArrayList<>();
    Connection connection = DataBaseUtil.getInstance();
    try {

      PreparedStatement preparedStatement = connection.prepareStatement(LIST_ALL_SQL);
      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        StreamingInfo streamingInfo = new StreamingInfo();

        streamingInfo.setId(resultSet.getInt("id"));
        streamingInfo.setAppName(resultSet.getString("app_name"));
        streamingInfo.setImageName(resultSet.getString("image_name"));
        streamingInfo.setImageTag(resultSet.getString("image_tag"));
        streamingInfo.setDriverCores(resultSet.getInt("driver_cores"));
        streamingInfo.setDriverMemory(resultSet.getString("driver_memory"));
        streamingInfo.setExecutorNums(resultSet.getInt("executor_nums"));
        streamingInfo.setExecutorCores(resultSet.getInt("executor_cores"));
        streamingInfo.setExecutorMems(resultSet.getString("executor_mems"));
        streamingInfo.setConfs(resultSet.getString("confs"));
        streamingInfo.setFiles(resultSet.getString("files"));
        streamingInfo.setQueue(resultSet.getString("queue"));
        streamingInfo.setClassName(resultSet.getString("class_name"));
        streamingInfo.setJarName(resultSet.getString("jar_name"));
        streamingInfo.setDepartment(resultSet.getString("department"));
        streamingInfo.setIsAvailable(resultSet.getInt("is_available"));
        streamingInfo.setAppId(resultSet.getString("app_id"));
        streamingInfo.setAppstate(resultSet.getInt("state"));
        streamingInfo.setCreateTime(resultSet.getDate("create_time"));
        streamingInfo.setLastUpdateTime(resultSet.getDate("last_update_time"));
        streamingInfo.setAlertState(resultSet.getInt("alert_state"));
        streamingInfo.setCreator(resultSet.getString("creator"));
        streamingInfo.setKiller(resultSet.getString("killer"));
        streamingInfo.setAlertAccumulation(resultSet.getInt("alert_accumulation"));
        streamingInfo.setAlertAccumulationThreshold(
            resultSet.getInt("alert_accumulation_threshold"));
        streamingInfo.setAlertActive(resultSet.getInt("alert_active"));
        streamingInfo.setAlertActiveProcessing(resultSet.getInt("alert_active_processing"));
        streamingInfo.setAlertActiveProcessingNumThreshold(
            resultSet.getInt("alert_active_processing_num_threshold"));
        streamingInfo.setAlertActiveProcessingTimeThreshold(
            resultSet.getInt("alert_active_processing_time_threshold"));
        streamingInfo.setAlertActiveThreshold(resultSet.getInt("alert_active_threshold"));
        streamingInfo.setAlertAlive(resultSet.getInt("alert_alive"));
        streamingInfo.setAlertError(resultSet.getInt("alert_error"));
        streamingInfo.setAlertErrorThreshold(resultSet.getInt("alert_error_threshold"));
        streamingInfo.setAlertGroup(resultSet.getString("alert_group"));
        streamingInfo.setAlertInactiveExecutors(resultSet.getInt("alert_inactive_executors"));
        streamingInfo.setAlertInactiveExecutorsThreshold(
            resultSet.getInt("alert_inactive_executors_threshold"));
        streamingInfo.setAlertInactiveReceivers(resultSet.getInt("alert_inactive_receivers"));
        streamingInfo.setAlertInactiveReceiversThreshold(
            resultSet.getInt("alert_inactive_receivers_threshold"));
        streamingInfo.setAlertInterval(resultSet.getInt("alert_interval"));
        streamingInfo.setAlertProcessing(resultSet.getInt("alert_processing"));
        streamingInfo.setAlertProcessingThreshold(resultSet.getInt("alert_processing_threshold"));
        streamingInfo.setAlertReceive(resultSet.getInt("alert_receive"));
        streamingInfo.setAlertReceiveThreshold(resultSet.getFloat("alert_receive_threshold"));
        /*streamingInfo.setAlertRecentlyReceive(resultSet.getInt("alert_recently_receive"));
        streamingInfo.setAlertRecentlyReceiveThreshold(
            resultSet.getInt("alert_recently_receive_threshold"));*/
        streamingInfo.setAlertRepetitive(resultSet.getInt("alert_repetitive"));
        streamingInfo.setAlertRepetitiveGroup(resultSet.getString("alert_repetitive_group"));
        streamingInfo.setAlertSchedulingDelay(resultSet.getInt("alert_scheduling_delay"));
        streamingInfo.setAlertSchedulingDelayThreshold(
            resultSet.getInt("alert_scheduling_delay_threshold"));
        streamingInfo.setCollection(resultSet.getInt("collection"));

        list.add(streamingInfo);
      }
      resultSet.close();
      preparedStatement.close();

    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return list;
  }

  /**
   * insert info.
   *
   * @param streamingInfo info
   */
  public int insert(StreamingInfo streamingInfo) {
    if (basicdbUtil.insert(getBasic(streamingInfo)) == 1) {
      if (alertdbUtil.insert(getAlert(streamingInfo)) == 0) {
        basicdbUtil.delete(streamingInfo.getAppName());
      } else {
        return 1;
      }
    }
    return 0;
  }

  /**
   * delete by name.
   *
   * @param name name
   */
  public int delete(String name) {
    int resault = 0;
    if (streamingdbUtil.selectByAppName(name) == null) {
      Connection connection = DataBaseUtil.getInstance();
      try {
        PreparedStatement preparedStatement = connection.prepareStatement(DELETE_BY_NAME);

        preparedStatement.setString(1, name);
        preparedStatement.setString(2, name);
        resault = preparedStatement.executeUpdate();

        preparedStatement.close();
      } catch (SQLException e) {
        LOGGER.error(ExceptionUtils.getFullStackTrace(e));
      }
      try {
        connection.close();
      } catch (SQLException e) {
        LOGGER.error(ExceptionUtils.getFullStackTrace(e));
      }
    }
    return resault;
  }

  /**
   * update info .
   *
   * @param streamingInfo info
   */
  public int update(StreamingInfo streamingInfo) {
    int resault = basicdbUtil.update(getBasic(streamingInfo));
    resault += alertdbUtil.update(getAlert(streamingInfo));
    return resault > 0 ? 1 : 0;
  }

  /**
   * find by name.
   *
   * @param name name
   */
  public StreamingInfo findByName(String name) {
    StreamingInfo streamingInfo = null;
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preparedStatement = connection.prepareStatement(FIND_BY_NAME);
      preparedStatement.setString(1, name);

      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        streamingInfo = new StreamingInfo();
        streamingInfo.setId(resultSet.getInt("id"));
        streamingInfo.setAppName(resultSet.getString("app_name"));
        streamingInfo.setImageName(resultSet.getString("image_name"));
        streamingInfo.setImageTag(resultSet.getString("image_tag"));
        streamingInfo.setDriverCores(resultSet.getInt("driver_cores"));
        streamingInfo.setDriverMemory(resultSet.getString("driver_memory"));
        streamingInfo.setExecutorNums(resultSet.getInt("executor_nums"));
        streamingInfo.setExecutorCores(resultSet.getInt("executor_cores"));
        streamingInfo.setExecutorMems(resultSet.getString("executor_mems"));
        streamingInfo.setConfs(resultSet.getString("confs"));
        streamingInfo.setFiles(resultSet.getString("files"));
        streamingInfo.setQueue(resultSet.getString("queue"));
        streamingInfo.setClassName(resultSet.getString("class_name"));
        streamingInfo.setJarName(resultSet.getString("jar_name"));
        streamingInfo.setDepartment(resultSet.getString("department"));
        streamingInfo.setIsAvailable(resultSet.getInt("is_available"));
        streamingInfo.setAppId(resultSet.getString("app_id"));
        streamingInfo.setAppstate(resultSet.getInt("state"));
        streamingInfo.setCreateTime(resultSet.getDate("create_time"));
        streamingInfo.setLastUpdateTime(resultSet.getDate("last_update_time"));
        streamingInfo.setAlertState(resultSet.getInt("alert_state"));
        streamingInfo.setCreator(resultSet.getString("creator"));
        streamingInfo.setKiller(resultSet.getString("killer"));
        streamingInfo.setAlertAccumulation(resultSet.getInt("alert_accumulation"));
        streamingInfo.setAlertAccumulationThreshold(
            resultSet.getInt("alert_accumulation_threshold"));
        streamingInfo.setAlertActive(resultSet.getInt("alert_active"));
        streamingInfo.setAlertActiveProcessing(resultSet.getInt("alert_active_processing"));
        streamingInfo.setAlertActiveProcessingNumThreshold(
            resultSet.getInt("alert_active_processing_num_threshold"));
        streamingInfo.setAlertActiveProcessingTimeThreshold(
            resultSet.getInt("alert_active_processing_time_threshold"));
        streamingInfo.setAlertActiveThreshold(resultSet.getInt("alert_active_threshold"));
        streamingInfo.setAlertAlive(resultSet.getInt("alert_alive"));
        streamingInfo.setAlertError(resultSet.getInt("alert_error"));
        streamingInfo.setAlertErrorThreshold(resultSet.getInt("alert_error_threshold"));
        streamingInfo.setAlertGroup(resultSet.getString("alert_group"));
        streamingInfo.setAlertInactiveExecutors(resultSet.getInt("alert_inactive_executors"));
        streamingInfo.setAlertInactiveExecutorsThreshold(
            resultSet.getInt("alert_inactive_executors_threshold"));
        streamingInfo.setAlertInactiveReceivers(resultSet.getInt("alert_inactive_receivers"));
        streamingInfo.setAlertInactiveReceiversThreshold(
            resultSet.getInt("alert_inactive_receivers_threshold"));
        streamingInfo.setAlertInterval(resultSet.getInt("alert_interval"));
        streamingInfo.setAlertProcessing(resultSet.getInt("alert_processing"));
        streamingInfo.setAlertProcessingThreshold(resultSet.getInt("alert_processing_threshold"));
        streamingInfo.setAlertReceive(resultSet.getInt("alert_receive"));
        streamingInfo.setAlertReceiveThreshold(resultSet.getInt("alert_receive_threshold"));
        /*streamingInfo.setAlertRecentlyReceive(resultSet.getInt("alert_recently_receive"));
        streamingInfo.setAlertRecentlyReceiveThreshold(
            resultSet.getFloat("alert_recently_receive_threshold"));*/
        streamingInfo.setAlertRepetitive(resultSet.getInt("alert_repetitive"));
        streamingInfo.setAlertRepetitiveGroup(resultSet.getString("alert_repetitive_group"));
        streamingInfo.setAlertSchedulingDelay(resultSet.getInt("alert_scheduling_delay"));
        streamingInfo.setAlertSchedulingDelayThreshold(
            resultSet.getInt("alert_scheduling_delay_threshold"));
        streamingInfo.setCollection(resultSet.getInt("collection"));
      }
      resultSet.close();
      preparedStatement.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return streamingInfo;
  }

  /**
   * exist or not.
   *
   * @param name name
   * @return true exist false not
   */
  public boolean isExit(String name) {
    return basicdbUtil.getByAppName(name) != null;
  }

  private Basic getBasic(StreamingInfo streamingInfo) {
    Basic basic = new Basic();
    basic.setAppName(streamingInfo.getAppName());
    basic.setImageName(streamingInfo.getImageName());
    basic.setImageTag(streamingInfo.getImageTag());
    basic.setDriverCores(streamingInfo.getDriverCores());
    basic.setDriverMemory(streamingInfo.getDriverMemory());
    basic.setExecutorNums(streamingInfo.getExecutorNums());
    basic.setExecutorCores(streamingInfo.getExecutorCores());
    basic.setExecutorMems(streamingInfo.getExecutorMems());
    basic.setConfs(streamingInfo.getConfs());
    basic.setFiles(streamingInfo.getFiles());
    basic.setQueue(streamingInfo.getQueue());
    basic.setClassName(streamingInfo.getClassName());
    basic.setJarName(streamingInfo.getJarName());
    basic.setDepartment(streamingInfo.getDepartment());
    basic.setIsAvailable(streamingInfo.getIsAvailable());
    return basic;
  }

  private Alert getAlert(StreamingInfo streamingInfo) {
    Alert alert = new Alert();
    alert.setAppName(streamingInfo.getAppName());
    alert.setCollection(streamingInfo.getCollection());
    alert.setAlertAlive(streamingInfo.getAlertAlive());
    alert.setAlertRepetitive(streamingInfo.getAlertRepetitive());
    alert.setAlertRepetitiveGroup(streamingInfo.getAlertRepetitiveGroup());
    alert.setAlertActive(streamingInfo.getAlertActive());
    alert.setAlertActiveThreshold(streamingInfo.getAlertActiveThreshold());
    alert.setAlertAccumulation(streamingInfo.getAlertAccumulation());
    alert.setAlertAccumulationThreshold(streamingInfo.getAlertAccumulationThreshold());
    alert.setAlertReceive(streamingInfo.getAlertReceive());
    alert.setAlertReceiveThreshold(streamingInfo.getAlertReceiveThreshold());
    alert.setAlertSchedulingDelay(streamingInfo.getAlertSchedulingDelay());
    alert.setAlertSchedulingDelayThreshold(streamingInfo.getAlertSchedulingDelayThreshold());
    alert.setAlertProcessing(streamingInfo.getAlertProcessing());
    alert.setAlertProcessingThreshold(streamingInfo.getAlertProcessingThreshold());
    alert.setAlertActiveProcessing(streamingInfo.getAlertActiveProcessing());
    alert.setAlertActiveProcessingTimeThreshold(
        streamingInfo.getAlertActiveProcessingTimeThreshold());
    alert.setAlertActiveProcessingNumThreshold(
        streamingInfo.getAlertActiveProcessingNumThreshold());
    alert.setAlertInactiveReceivers(streamingInfo.getAlertInactiveReceivers());
    alert.setAlertInactiveReceiversThreshold(streamingInfo.getAlertInactiveReceiversThreshold());
    alert.setAlertInactiveExecutors(streamingInfo.getAlertInactiveExecutors());
    alert.setAlertInactiveExecutorsThreshold(streamingInfo.getAlertInactiveExecutorsThreshold());
    alert.setAlertError(streamingInfo.getAlertError());
    alert.setAlertErrorThreshold(streamingInfo.getAlertErrorThreshold());
    alert.setAlertInterval(streamingInfo.getAlertInterval());
    alert.setAlertGroup(streamingInfo.getAlertGroup());
    alert.setState(streamingInfo.getAlertState());
    return alert;
  }
}
