package com.weibo.dip.data.platform.datacubic.business.summonagg.dao;

import com.weibo.dip.data.platform.datacubic.business.summonagg.service.impl.KafkaServiceImpl;
import com.weibo.dip.data.platform.datacubic.business.util.AggConstants;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liyang28 on 2018/7/29.
 */
public class AggStatus {

  private static final Logger LOGGER = LoggerFactory.getLogger(AggStatus.class);

  /**
   * mysql 保存状态 .
   *
   * @param index .
   * @param produceCount .
   */
  public static void sendMysqlStatus(String index, long produceCount) {
    Connection connection = null;
    try {
      Class.forName(AggConstants.DRIVER);
      connection = DriverManager
          .getConnection(AggConstants.URL, AggConstants.USERNAME, AggConstants.PASSWORD);
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery(AggConstants.getSql(index));
      if (rs.next()) {
        PreparedStatement preparedStatement = connection.prepareStatement(AggConstants.UPDATE_SQL);
        preparedStatement.setInt(1, 1);
        preparedStatement.setInt(2, 0);
        preparedStatement.setLong(3, produceCount);
        preparedStatement.setLong(4, 0);
        preparedStatement.setString(5, index);
        preparedStatement.executeUpdate();
      } else {
        PreparedStatement preparedStatement = connection.prepareStatement(AggConstants.INSERT_SQL);
        preparedStatement.setString(1, index);
        preparedStatement.setInt(2, 1);
        preparedStatement.setInt(3, 0);
        preparedStatement.setLong(4, produceCount);
        preparedStatement.setLong(5, 0);
        preparedStatement.execute();
      }
      rs.close();
      stmt.close();
    } catch (SQLException e) {
      LOGGER.error("sql Exception: {}", ExceptionUtils.getFullStackTrace(e));
    } catch (ClassNotFoundException e) {
      LOGGER.error("sql class not found Exception: {}", ExceptionUtils.getFullStackTrace(e));
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          LOGGER.error("sql connection close Exception: {}", ExceptionUtils.getFullStackTrace(e));
        }
      }
    }

  }

}
