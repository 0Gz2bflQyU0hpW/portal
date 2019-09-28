package com.weibo.dip.data.platform.falcon.kafka.cost;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jianhong
 * @date 2018/4/24
 */
public class MySQLService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLService.class);

  private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
  private static final String DB_URL = "jdbc:mysql://d136092.innet.dip.weibo.com:3307/kafka?characterEncoding=utf-8";

  private static final String USER = "root";
  private static final String PASSWORD = "mysqladmin";

  /**
   * 返回所有的topic及对应的产品线
   * @return
   * @throws IOException
   */
  public Map<String, String> getTopicProduct() {
    Connection conn = null;
    Statement statement = null;
    Map<String, String> topicProduceMap = new HashMap<>();

    try {
      Class.forName(JDBC_DRIVER);
      conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
      statement = conn.createStatement();

      ResultSet rs = statement.executeQuery("select topic_name, product_uuid from topic");

      while (rs.next()){
        String topicName = rs.getString(1);
        String productUuid = rs.getString(2);
        topicProduceMap.put(topicName, productUuid);
      }
    } catch (Exception e) {
      LOGGER.error("execute sql error\n" + ExceptionUtils.getFullStackTrace(e));
    } finally {
      try {
        if (statement != null)
          statement.close();
      } catch (SQLException e) {
        LOGGER.error("close statement error\n" + ExceptionUtils.getFullStackTrace(e));
      }
      try {
        if (conn != null)
          conn.close();
      } catch (SQLException e) {
        LOGGER.error("close connection error\n" + ExceptionUtils.getFullStackTrace(e));
      }
    }

    return topicProduceMap;
  }
}
