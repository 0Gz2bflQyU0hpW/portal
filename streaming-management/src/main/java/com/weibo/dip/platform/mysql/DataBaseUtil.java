package com.weibo.dip.platform.mysql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataBaseUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataBaseUtil.class);

  private static final String USERNAME = "root";
  private static final String PASSWORD = "mysqladmin";
  private static final String DBNAME = "portal";
  private static final String DRIVER = "com.mysql.jdbc.Driver";
  private static final String URL =
      "jdbc:mysql://d136092.innet.dip.weibo.com:3307/"
          + DBNAME
          + "?characterEncoding=UTF-8&useSSL=false"
          + "&autoReconnect=true&autoReconnectForPools=true";
  // 初始线程池大小
  private static final int POOLSIZE = 30;
  // 最大
  private static final int MAXSIZE = 200;
  // 最小
  private static final int MINSIZE = 30;
  // 每次增长个数
  private static final int INCREMENT = 30;

  private static final int TESTPERIOD = 3600;

  private static final int CONTIMEOUT = 180;

  private static final int TIMEOUT = 10000;

  // private static final int CONNECTIONAGE = 120000;

  private static ComboPooledDataSource dataSource = new ComboPooledDataSource();

  static {
    try {
      dataSource.setDriverClass(DRIVER);
      dataSource.setJdbcUrl(URL);
      dataSource.setUser(USERNAME);
      dataSource.setPassword(PASSWORD);

      // 可以使用默认值
      dataSource.setInitialPoolSize(POOLSIZE);
      dataSource.setMaxPoolSize(MAXSIZE);
      dataSource.setMinPoolSize(MINSIZE);
      dataSource.setAcquireIncrement(INCREMENT);
      // 测试时间，单位秒,设定为一个小时测试一次
      dataSource.setIdleConnectionTestPeriod(TESTPERIOD);
      // 为了性能不用out
      dataSource.setTestConnectionOnCheckin(true);
      // 连接未归还到连接池的时效长度，单位秒
      dataSource.setUnreturnedConnectionTimeout(CONTIMEOUT);
      // 连接耗尽的等待时间，单位毫秒
      dataSource.setCheckoutTimeout(TIMEOUT);
      // 配置连接的生存时间，超过这个时间的连接将由连接池自动断开丢弃掉
      /*dataSource.setMaxConnectionAge(CONNECTIONAGE);*/
    } catch (PropertyVetoException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
  }

  private DataBaseUtil() {}

  /**
   * get connection.
   *
   * @return connection
   */
  public static Connection getInstance() {
    try {
      return dataSource.getConnection();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
      return null;
    }
  }
}
