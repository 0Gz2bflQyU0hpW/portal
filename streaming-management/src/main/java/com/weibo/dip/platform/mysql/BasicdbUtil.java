package com.weibo.dip.platform.mysql;

import com.weibo.dip.data.platform.commons.Symbols;
import com.weibo.dip.platform.model.Basic;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicdbUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(BasicdbUtil.class);

  private static final String INSERT_SQL =
      "INSERT INTO streaming_basic ("
          + "app_name, "
          + "image_name, "
          + "image_tag, "
          + "driver_cores, "
          + "driver_memory, "
          + "executor_nums, "
          + "executor_cores, "
          + "executor_mems, "
          + "confs, "
          + "files, "
          + "queue, "
          + "class_name, "
          + "jar_name, "
          + "department) "
          + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

  private static final String UPDATE_SQL =
      "UPDATE streaming_basic SET "
          + "driver_cores=?, "
          + "driver_memory=?,"
          + "executor_nums=?, "
          + "executor_cores=?, "
          + "executor_mems=?, "
          + "confs=?, "
          + "files=?, "
          + "queue=?, "
          + "class_name=?, "
          + "jar_name=?, "
          + "department=?, "
          + "is_available=? "
          + "WHERE app_name=?";

  private static final String SELECT_BY_ID_SQL = "SELECT * FROM streaming_basic WHERE id=?";

  private static final String SELECT_BY_APPNAME_SQL =
      "SELECT * FROM streaming_basic WHERE app_name=? ORDER BY id desc limit 1";

  private static final String SELECT_BASICINFO_SQL =
      "SELECT * FROM streaming_basic WHERE app_name=? AND image_name=? AND image_tag=?";

  private static final String DELETE_SQL = "DELETE FROM streaming_basic WHERE id=?";

  private static final String DELETE_BY_NAME = "DELETE FROM streaming_basic WHERE app_name=?";

  /**
   * insert basic info.
   *
   * @param basic basic ifo
   */
  public int insert(Basic basic) {
    int resault = 0;
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preStmt = connection.prepareStatement(INSERT_SQL);

      preStmt.setString(1, basic.getAppName());
      preStmt.setString(2, basic.getImageName());
      preStmt.setString(3, basic.getImageTag());
      preStmt.setInt(4, basic.getDriverCores());
      preStmt.setString(5, basic.getDriverMemory());
      preStmt.setInt(6, basic.getExecutorNums());
      preStmt.setInt(7, basic.getExecutorCores());
      preStmt.setString(8, basic.getExecutorMems());
      preStmt.setString(9, basic.getConfs());
      preStmt.setString(10, basic.getFiles());
      preStmt.setString(11, basic.getQueue());
      preStmt.setString(12, basic.getClassName());
      preStmt.setString(13, basic.getJarName());
      preStmt.setString(14, basic.getDepartment());
      resault = preStmt.executeUpdate();
      preStmt.close();
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
   * update by basic include all info.
   *
   * @param basic basic info
   */
  public int update(Basic basic) {
    int resault = 0;
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preStmt = connection.prepareStatement(UPDATE_SQL);
      preStmt.setInt(1, basic.getDriverCores());
      preStmt.setString(2, basic.getDriverMemory());
      preStmt.setInt(3, basic.getExecutorNums());
      preStmt.setInt(4, basic.getExecutorCores());
      preStmt.setString(5, basic.getExecutorMems());
      preStmt.setString(6, basic.getConfs());
      preStmt.setString(7, basic.getFiles());
      preStmt.setString(8, basic.getQueue());
      preStmt.setString(9, basic.getClassName());
      preStmt.setString(10, basic.getJarName());
      preStmt.setString(11, basic.getDepartment());
      preStmt.setInt(12, basic.getIsAvailable());
      preStmt.setString(13, basic.getAppName());
      resault = preStmt.executeUpdate();
      preStmt.close();
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
   * get Basic by id.
   *
   * @param id id
   */
  public Basic getById(int id) {
    Basic basic = null;
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preStmt = connection.prepareStatement(SELECT_BY_ID_SQL);
      preStmt.setInt(1, id);
      ResultSet rs = preStmt.executeQuery();
      while (rs.next()) {
        basic = new Basic();
        basic.setId(rs.getInt("id"));
        basic.setAppName(rs.getString("app_name"));
        basic.setImageName(rs.getString("image_name"));
        basic.setImageTag(rs.getString("image_tag"));
        basic.setDriverCores(rs.getInt("driver_cores"));
        basic.setDriverMemory(rs.getString("driver_memory"));
        basic.setExecutorNums(rs.getInt("executor_nums"));
        basic.setExecutorCores(rs.getInt("executor_cores"));
        basic.setExecutorMems(rs.getString("executor_mems"));
        basic.setConfs(rs.getString("confs"));
        basic.setFiles(rs.getString("files"));
        basic.setQueue(rs.getString("queue"));
        basic.setClassName(rs.getString("class_name"));
        basic.setJarName(rs.getString("jar_name"));
        basic.setDepartment(rs.getString("department"));
        basic.setIsAvailable(rs.getInt("is_available"));
      }
      rs.close();
      preStmt.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return basic;
  }

  /**
   * get Basic by app name.
   *
   * @param appName app name
   */
  public Basic getByAppName(String appName) {
    Basic basic = null;
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preStmt = connection.prepareStatement(SELECT_BY_APPNAME_SQL);
      preStmt.setString(1, appName);
      ResultSet rs = preStmt.executeQuery();
      while (rs.next()) {
        basic = new Basic();
        basic.setId(rs.getInt("id"));
        basic.setAppName(rs.getString("app_name"));
        basic.setImageName(rs.getString("image_name"));
        basic.setImageTag(rs.getString("image_tag"));
        basic.setDriverCores(rs.getInt("driver_cores"));
        basic.setDriverMemory(rs.getString("driver_memory"));
        basic.setExecutorNums(rs.getInt("executor_nums"));
        basic.setExecutorCores(rs.getInt("executor_cores"));
        basic.setExecutorMems(rs.getString("executor_mems"));
        basic.setConfs(rs.getString("confs"));
        basic.setFiles(rs.getString("files"));
        basic.setQueue(rs.getString("queue"));
        basic.setClassName(rs.getString("class_name"));
        basic.setJarName(rs.getString("jar_name"));
        basic.setDepartment(rs.getString("department"));
        basic.setIsAvailable(rs.getInt("is_available"));
      }
      rs.close();
      preStmt.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return basic;
  }

  /**
   * get info .
   *
   * @param appName app name
   * @param imageName image name
   * @param imageTag image tag
   */
  public Basic getBasicInfo(String appName, String imageName, String imageTag) {
    Basic basic = null;
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preStmt = connection.prepareStatement(SELECT_BASICINFO_SQL);
      preStmt.setString(1, appName);
      preStmt.setString(2, imageName);
      preStmt.setString(3, imageTag);
      ResultSet rs = preStmt.executeQuery();
      while (rs.next()) {
        basic = new Basic();
        basic.setId(rs.getInt("id"));
        basic.setAppName(rs.getString("app_name"));
        basic.setImageName(rs.getString("image_name"));
        basic.setImageTag(rs.getString("image_tag"));
        basic.setDriverCores(rs.getInt("driver_cores"));
        basic.setDriverMemory(rs.getString("driver_memory"));
        basic.setExecutorNums(rs.getInt("executor_nums"));
        basic.setExecutorCores(rs.getInt("executor_cores"));
        basic.setExecutorMems(rs.getString("executor_mems"));
        basic.setConfs(rs.getString("confs"));
        basic.setFiles(rs.getString("files"));
        basic.setQueue(rs.getString("queue"));
        basic.setClassName(rs.getString("class_name"));
        basic.setJarName(rs.getString("jar_name"));
        basic.setDepartment(rs.getString("department"));
        basic.setIsAvailable(rs.getInt("is_available"));
      }
      rs.close();
      preStmt.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return basic;
  }

  /**
   * delete by id.
   *
   * @param id id
   */
  public int delete(int id) {
    int resault = 0;
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preStmt = connection.prepareStatement(DELETE_SQL);
      preStmt.setInt(1, id);
      resault = preStmt.executeUpdate();
      preStmt.close();
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
   * delete by name.
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
}
