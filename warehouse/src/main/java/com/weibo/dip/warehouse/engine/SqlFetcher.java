package com.weibo.dip.warehouse.engine;

import com.weibo.dip.data.platform.commons.ClasspathProperties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sql fetcher.
 *
 * @author yurun
 */
public class SqlFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlFetcher.class);

  private String driver;
  private String url;
  private String username;
  private String password;

  /**
   * Construct a instance.
   *
   * @param properties properties
   */
  public SqlFetcher(ClasspathProperties properties) {
    driver = properties.getString("db.driver");
    url = properties.getString("db.url");
    username = properties.getString("db.username");
    password = properties.getString("db.password");
  }

  /**
   * Fetch sqls by name.
   *
   * @param name name
   * @return sqls
   * @throws Exception if fetch error
   */
  public String[] fetch(String name) throws Exception {
    String query = "select sqls from hiveapps where name = ?";

    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;

    try {
      Class.forName(driver);

      conn = DriverManager.getConnection(url, username, password);

      stmt = conn.prepareStatement(query);
      stmt.setString(1, name);

      rs = stmt.executeQuery();

      return rs.next() ? rs.getString("sqls").split(";", -1) : null;
    } finally {
      if (Objects.nonNull(rs)) {
        try {
          rs.close();
        } catch (SQLException e) {
          LOGGER.error("rs close error: {}", ExceptionUtils.getFullStackTrace(e));
        }
      }

      if (Objects.nonNull(stmt)) {
        try {
          stmt.close();
        } catch (SQLException e) {
          LOGGER.error("stmt close error: {}", ExceptionUtils.getFullStackTrace(e));
        }
      }

      if (Objects.nonNull(conn)) {
        try {
          conn.close();
        } catch (Exception e) {
          LOGGER.error("conn close error: {}", ExceptionUtils.getFullStackTrace(e));
        }
      }
    }
  }
}
