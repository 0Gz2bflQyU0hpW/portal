package com.weibo.dip.data.platform.falcon.kafka.cost;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jianhong
 * @date 2018/4/23
 */
public class CostAccountMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(CostAccountMain.class);

  private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
  private static final String DB_URL = "jdbc:mysql://m7766i.mars.grid.sina.com.cn:7766/dip_data_analyze?characterEncoding=utf-8";

  private static final String USER = "dipadmin6103";
  private static final String PASSWORD = "702f23bc282c374";

  public static void main(String[] args) {
    RestService restService = new RestService();
    String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis() - 86400000);
    LOGGER.info("kafka cost account date: " + yesterday);

    Connection conn = null;
    Statement statement = null;
    int count = 0;

    try {
      Map<String, String> topicProduceMap = new MySQLService().getTopicProduct();
      LOGGER.info("topic stores in mysql, amount: " + topicProduceMap.size());

      Class.forName(JDBC_DRIVER);
      conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
      statement = conn.createStatement();

      Map<String, BigDecimal> topicBytesOutMap =  restService.getTopicBytesOut(yesterday);
      LOGGER.info("active topic in kafka, amount: " + topicBytesOutMap.size());

      for (Entry<String, BigDecimal> entry: topicBytesOutMap.entrySet()) {
        String topic = entry.getKey();
        BigDecimal bytesOut = entry.getValue();
        String productUuid = topicProduceMap.get(topic);

        //不计算没有产品线的和本部门的成本
        if (productUuid != null && !productUuid.equals("2012110217554677")) {
          String sql = "INSERT INTO kafka_resource_statistics(topic_name, product_uuid, peak_bytes_out) "
              + "VALUES('" + topic + "', '" + productUuid + "', '" + bytesOut.doubleValue() + "')";

          statement.executeUpdate(sql);

//          test output
//          System.out.println(topic + "\t" + productUuid + "\t" + bytesOut.doubleValue());
          count++;
        }
      }
      LOGGER.info("insert into mysql record count: " + count);
    } catch (Exception e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
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
  }
}
