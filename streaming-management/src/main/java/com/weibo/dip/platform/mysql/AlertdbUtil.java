package com.weibo.dip.platform.mysql;

import com.weibo.dip.platform.model.Alert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertdbUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlertdbUtil.class);

  private static final String INSERT_SQL =
      "insert into streaming_alert("
          + "app_name,"
          + "collection,"
          + "alert_alive,"
          + "alert_repetitive,"
          + "alert_repetitive_group,"
          + "alert_active,"
          + "alert_active_threshold,"
          + "alert_accumulation,"
          + "alert_accumulation_threshold,"
          + "alert_receive,"
          + "alert_receive_threshold,"
          + "alert_scheduling_delay,"
          + "alert_scheduling_delay_threshold,"
          + "alert_processing,"
          + "alert_processing_threshold,"
          + "alert_active_processing,"
          + "alert_active_processing_time_threshold,"
          + "alert_active_processing_num_threshold,"
          + "alert_inactive_receivers,"
          + "alert_inactive_receivers_threshold,"
          + "alert_inactive_executors,"
          + "alert_inactive_executors_threshold,"
          + "alert_error,"
          + "alert_error_threshold,"
          + "alert_interval,"
          + "alert_group) "
          + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

  private static final String UPDATE_SQL =
      "update streaming_alert "
          + "set collection=?,"
          + "alert_alive=?,"
          + "alert_repetitive=?,"
          + "alert_repetitive_group=?,"
          + "alert_active=?,"
          + "alert_active_threshold=?,"
          + "alert_accumulation=?,"
          + "alert_accumulation_threshold=?,"
          + "alert_receive=?,"
          + "alert_receive_threshold=?,"
          + "alert_scheduling_delay=?,"
          + "alert_scheduling_delay_threshold=?,"
          + "alert_processing=?,"
          + "alert_processing_threshold=?,"
          + "alert_active_processing=?,"
          + "alert_active_processing_time_threshold=?,"
          + "alert_active_processing_num_threshold=?,"
          + "alert_inactive_receivers=?,"
          + "alert_inactive_receivers_threshold=?,"
          + "alert_inactive_executors=?,"
          + "alert_inactive_executors_threshold=?,"
          + "alert_error=?,"
          + "alert_error_threshold=?,"
          + "alert_interval=?,"
          + "alert_group=? "
          + "where app_name = ? ";

  private static final String UPDATE_STATE_BY_APPNAME_SQL =
      "update streaming_alert set state =? where app_name =?";

  private static final String SELECT_SQL = "select * from streaming_alert where state=1";

  private static final String DELETE_BY_NAME = "DELETE FROM streaming_alert WHERE app_name=?";

  /**
   * list all alert info.
   *
   * @return not null
   */
  public List<Alert> getAlertInfos() {
    List<Alert> alerts = new ArrayList<>();
    Connection connection = DataBaseUtil.getInstance();
    try {

      PreparedStatement preparedStatement = connection.prepareStatement(SELECT_SQL);
      ResultSet resultSet = preparedStatement.executeQuery();

      while (resultSet.next()) {
        Alert alert = new Alert();
        alert.setId(resultSet.getInt("id"));
        alert.setAppName(resultSet.getString("app_name"));
        alert.setCollection(resultSet.getInt("collection"));
        alert.setAlertAlive(resultSet.getInt("alert_alive"));
        alert.setRestartCmd(resultSet.getString("restart_cmd"));
        alert.setAlertRepetitive(resultSet.getInt("alert_repetitive"));
        alert.setAlertRepetitiveGroup(resultSet.getString("alert_repetitive_group"));
        alert.setAlertActive(resultSet.getInt("alert_active"));
        alert.setAlertActiveThreshold(resultSet.getInt("alert_active_threshold"));
        alert.setAlertAccumulation(resultSet.getInt("alert_accumulation"));
        alert.setAlertAccumulationThreshold(resultSet.getInt("alert_accumulation_threshold"));
        alert.setAlertReceive(resultSet.getInt("alert_receive"));
        alert.setAlertReceiveThreshold(resultSet.getFloat("alert_receive_threshold"));
        alert.setAlertSchedulingDelay(resultSet.getInt("alert_scheduling_delay"));
        alert.setAlertSchedulingDelayThreshold(
            resultSet.getInt("alert_scheduling_delay_threshold"));
        alert.setAlertProcessing(resultSet.getInt("alert_processing"));
        alert.setAlertProcessingThreshold(resultSet.getInt("alert_processing_threshold"));
        alert.setAlertActiveProcessing(resultSet.getInt("alert_active_processing"));
        alert.setAlertActiveProcessingTimeThreshold(
            resultSet.getInt("alert_active_processing_time_threshold"));
        alert.setAlertActiveProcessingNumThreshold(
            resultSet.getInt("alert_active_processing_num_threshold"));
        alert.setAlertInactiveReceivers(resultSet.getInt("alert_inactive_receivers"));
        alert.setAlertInactiveReceiversThreshold(
            resultSet.getInt("alert_inactive_receivers_threshold"));
        alert.setAlertInactiveExecutors(resultSet.getInt("alert_inactive_executors"));
        alert.setAlertInactiveExecutorsThreshold(
            resultSet.getInt("alert_inactive_executors_threshold"));
        alert.setAlertError(resultSet.getInt("alert_error"));
        alert.setAlertErrorThreshold(resultSet.getInt("alert_error_threshold"));
        alert.setAlertInterval(resultSet.getInt("alert_interval"));
        alert.setAlertGroup(resultSet.getString("alert_group"));
        alert.setState(resultSet.getInt("state"));
        alerts.add(alert);
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
    return alerts;
  }

  /**
   * update state.
   *
   * @param appName appname
   * @param state state
   */
  public void updateStateByAppName(int state, String appName) {
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preStmt = connection.prepareStatement(UPDATE_STATE_BY_APPNAME_SQL);
      preStmt.setInt(1, state);
      preStmt.setString(2, appName);
      preStmt.executeUpdate();
      preStmt.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * .
   * @param id .
   * @param specifiedColumn .
   * @param value .
   */
  public void updateSpecifiedColumnById(int id, String specifiedColumn, int value) {
    String sql = "update streaming_alert set " + specifiedColumn + " =? where id =?";
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preStmt = connection.prepareStatement(sql);
      preStmt.setInt(1, value);
      preStmt.setInt(2, id);
      preStmt.executeUpdate();
      preStmt.close();
    } catch (Exception e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * delete .
   *
   * @param name name
   */
  public int delete(String name) {
    int resault = 0;
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preparedStatement = connection.prepareStatement(DELETE_BY_NAME);
      preparedStatement.setString(1, name);
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
    return resault;
  }

  /**
   * update.
   *
   * @param alert alert info
   */
  public int update(Alert alert) {
    int resault = 0;
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_SQL);
      preparedStatement.setInt(1, alert.getCollection());
      preparedStatement.setInt(2, alert.getAlertAlive());
      preparedStatement.setInt(3, alert.getAlertRepetitive());
      preparedStatement.setString(4, alert.getAlertRepetitiveGroup());
      preparedStatement.setInt(5, alert.getAlertActive());
      preparedStatement.setInt(6, alert.getAlertActiveThreshold());
      preparedStatement.setInt(7, alert.getAlertAccumulation());
      preparedStatement.setInt(8, alert.getAlertAccumulationThreshold());
      preparedStatement.setInt(9, alert.getAlertReceive());
      preparedStatement.setFloat(10, alert.getAlertReceiveThreshold());
      preparedStatement.setInt(11, alert.getAlertSchedulingDelay());
      preparedStatement.setInt(12, alert.getAlertSchedulingDelayThreshold());
      preparedStatement.setInt(13, alert.getAlertProcessing());
      preparedStatement.setInt(14, alert.getAlertProcessingThreshold());
      preparedStatement.setInt(15, alert.getAlertActiveProcessing());
      preparedStatement.setInt(16, alert.getAlertActiveProcessingTimeThreshold());
      preparedStatement.setInt(17, alert.getAlertActiveProcessingNumThreshold());
      preparedStatement.setInt(18, alert.getAlertInactiveReceivers());
      preparedStatement.setInt(19, alert.getAlertInactiveReceiversThreshold());
      preparedStatement.setInt(20, alert.getAlertInactiveExecutors());
      preparedStatement.setInt(21, alert.getAlertInactiveExecutorsThreshold());
      preparedStatement.setInt(22, alert.getAlertError());
      preparedStatement.setInt(23, alert.getAlertErrorThreshold());
      preparedStatement.setInt(24, alert.getAlertInterval());
      preparedStatement.setString(25, alert.getAlertGroup());
      preparedStatement.setString(26, alert.getAppName());
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
    return resault;
  }

  /**
   * insert.
   *
   * @param alert alert info
   */
  public int insert(Alert alert) {
    int resault = 0;
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SQL);

      preparedStatement.setString(1, alert.getAppName());
      preparedStatement.setInt(2, alert.getCollection());
      preparedStatement.setInt(3, alert.getAlertAlive());
      preparedStatement.setInt(4, alert.getAlertRepetitive());
      preparedStatement.setString(5, alert.getAlertRepetitiveGroup());
      preparedStatement.setInt(6, alert.getAlertActive());
      preparedStatement.setInt(7, alert.getAlertActiveThreshold());
      preparedStatement.setInt(8, alert.getAlertAccumulation());
      preparedStatement.setInt(9, alert.getAlertAccumulationThreshold());
      preparedStatement.setInt(10, alert.getAlertReceive());
      preparedStatement.setFloat(11, alert.getAlertReceiveThreshold());
      preparedStatement.setInt(12, alert.getAlertSchedulingDelay());
      preparedStatement.setInt(13, alert.getAlertSchedulingDelayThreshold());
      preparedStatement.setInt(14, alert.getAlertProcessing());
      preparedStatement.setInt(15, alert.getAlertProcessingThreshold());
      preparedStatement.setInt(16, alert.getAlertActiveProcessing());
      preparedStatement.setInt(17, alert.getAlertActiveProcessingTimeThreshold());
      preparedStatement.setInt(18, alert.getAlertActiveProcessingNumThreshold());
      preparedStatement.setInt(19, alert.getAlertInactiveReceivers());
      preparedStatement.setInt(20, alert.getAlertInactiveReceiversThreshold());
      preparedStatement.setInt(21, alert.getAlertInactiveExecutors());
      preparedStatement.setInt(22, alert.getAlertInactiveExecutorsThreshold());
      preparedStatement.setInt(23, alert.getAlertError());
      preparedStatement.setInt(24, alert.getAlertErrorThreshold());
      preparedStatement.setInt(25, alert.getAlertInterval());
      preparedStatement.setString(26, alert.getAlertGroup());

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
    return resault;
  }
}
