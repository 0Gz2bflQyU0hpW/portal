package com.weibo.dip.platform.mysql;

import com.weibo.dip.platform.model.StateEnum;
import com.weibo.dip.platform.model.Streaming;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingdbUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingdbUtil.class);

  private static final DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  // private Connection connection = DataBaseUtil.getInstance();

  private static final String INSERT_SQL =
      "INSERT INTO streaming_appinfo (app_name,state,create_time,last_update_time,creator) "
          + "SELECT ?,?,?,?,? FROM dual "
          + "WHERE NOT EXISTS(SELECT *  FROM streaming_appinfo "
          + "WHERE app_name = ? "
          + ")";

  private static final String SELECT_BY_ID_SQL = "SELECT * FROM streaming_appinfo WHERE id =?";

  private static final String SELECT_SQL = "SELECT * FROM streaming_appinfo";

  private static final String INSERT_HISTORY =
      "insert into streaming_history"
          + "(app_name,state,app_id,create_time,last_update_time,creator,killer) "
          + "values (?,?,?,?,?,?,?)";

  private static final String DELETE_BY_ID = "delete from streaming_appinfo where id =?";

  private static final String SELECT_BY_NAME = "select * from streaming_appinfo where app_name =?";

  private static final String SELECT_ALL_WAIT =
      "select * from streaming_appinfo where state=? or state=? or state=?";

  /**
   * insert .
   *
   * @param appName name
   * @return line number
   */
  public int insertNotExists(String appName, String creator) {
    int resault = 0;
    int id = 0;
    Connection connection = DataBaseUtil.getInstance();
    try {

      PreparedStatement preStmt =
          connection.prepareStatement(INSERT_SQL, Statement.RETURN_GENERATED_KEYS);
      preStmt.setString(1, appName);
      preStmt.setInt(2, StateEnum.WAITSTART.getNumber());
      preStmt.setString(3, dateformat.format(new Date().getTime()));
      preStmt.setString(4, dateformat.format(new Date().getTime()));
      preStmt.setString(5, creator);
      preStmt.setString(6, appName);
      resault = preStmt.executeUpdate();

      /*ResultSet rs = preStmt.getGeneratedKeys();
      rs.next();
      id = rs.getInt(1);
      rs.close();*/
      preStmt.close();
    } catch (SQLException e) {
      LOGGER.error("insert {} into DB error. {}", appName, ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    // return id;
    return resault;
  }

  /**
   * update with appid.
   *
   * @param id id
   * @param state state
   * @param states stateset
   * @param appId appid
   * @return line number
   */
  public int updateState(int id, int state, List<Integer> states, String appId) {
    int result = 0;
    Connection connection = DataBaseUtil.getInstance();
    try {
      PreparedStatement preStmt = connection.prepareStatement(getSql(appId, states));
      preStmt.setInt(1, state);
      preStmt.setString(2, dateformat.format(new Date().getTime()));
      preStmt.setString(3, appId);
      preStmt.setInt(4, id);
      result = preStmt.executeUpdate();
      preStmt.close();
    } catch (SQLException e) {
      LOGGER.error(
          "update streaming state error, streaming id: {}, error message: {}",
          id,
          ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return result;
  }

  /**
   * update without appid.
   *
   * @param id id
   * @param state state
   * @param states stateset
   * @return line number
   */
  public int updateState(int id, int state, List<Integer> states) {
    int result = 0;
    Connection connection = DataBaseUtil.getInstance();
    try {

      PreparedStatement preStmt = connection.prepareStatement(getSql(null, states));
      preStmt.setInt(1, state);
      preStmt.setString(2, dateformat.format(new Date().getTime()));
      preStmt.setInt(3, id);
      result = preStmt.executeUpdate();
      preStmt.close();
    } catch (SQLException e) {
      LOGGER.error(
          "update streaming state error, streaming id: {}, error message: {}",
          id,
          ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return result;
  }

  private String getSql(String appId, List<Integer> states) {
    String sql = "UPDATE streaming_appinfo SET state=?" + ", last_update_time=? ";
    if (appId != null && !"".equals(appId)) {
      sql += ", app_id=? ";
    }
    sql += " WHERE id = ? ";
    if (states != null && states.size() > 0) {
      sql += "and state in (" + StringUtils.join(states, ", ") + ")";
    }
    return sql;
  }

  /**
   * select by id .
   *
   * @param id id
   * @return info
   */
  public Streaming selectById(int id) {
    Streaming streaming = null;
    Connection connection = DataBaseUtil.getInstance();
    try {

      PreparedStatement preStmt = connection.prepareStatement(SELECT_BY_ID_SQL);
      preStmt.setInt(1, id);
      ResultSet rs = preStmt.executeQuery();

      while (rs.next()) {
        streaming = new Streaming();
        streaming.setId(rs.getInt("id"));
        streaming.setAppName(rs.getString("app_name"));
        streaming.setAppId(rs.getString("app_id"));
        streaming.setState(rs.getInt("state"));
        streaming.setLastUpdateTime(rs.getTimestamp("last_update_time"));
        streaming.setCreateTime(rs.getTimestamp("create_time"));
      }
      rs.close();
      preStmt.close();

    } catch (SQLException e) {
      LOGGER.error(
          "select by id error, streaming id: {}, error message: {}",
          id,
          ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return streaming;
  }

  /**
   * add history.
   *
   * @param streaming 要插入的信息
   * @return 受影响行数
   */
  public int insertHistory(Streaming streaming) {
    int result = 0;
    Connection connection = DataBaseUtil.getInstance();
    try {

      PreparedStatement preparedStatement = connection.prepareStatement(INSERT_HISTORY);
      preparedStatement.setString(1, streaming.getAppName());
      preparedStatement.setInt(2, streaming.getState());
      preparedStatement.setString(3, streaming.getAppId());
      preparedStatement.setString(4, dateformat.format(streaming.getCreateTime()));
      preparedStatement.setString(5, dateformat.format(streaming.getLastUpdateTime()));
      preparedStatement.setString(6, streaming.getCreator());
      preparedStatement.setString(7, streaming.getKiller());
      result = preparedStatement.executeUpdate();
      preparedStatement.close();
    } catch (SQLException e) {
      LOGGER.error(
          "insert history error, streaming name: {}, error message: {}",
          streaming.getAppName(),
          ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return result;
  }

  /**
   * 通过id删除.
   *
   * @param id ID
   * @return 受影响行数
   */
  public int deleteById(int id) {
    int result = 0;
    Connection connection = DataBaseUtil.getInstance();
    try {

      PreparedStatement preparedStatement = connection.prepareStatement(DELETE_BY_ID);
      preparedStatement.setInt(1, id);
      result = preparedStatement.executeUpdate();
      preparedStatement.close();
    } catch (SQLException e) {
      LOGGER.error(
          "delete by id error, streaming id: {}, error message: {}",
          id,
          ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return result;
  }

  /**
   * select.
   *
   * @param appName app's name
   */
  public Streaming selectByAppName(String appName) {
    Streaming streaming = null;
    Connection connection = DataBaseUtil.getInstance();
    try {

      PreparedStatement preparedStatement = connection.prepareStatement(SELECT_BY_NAME);
      preparedStatement.setString(1, appName);
      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        streaming = new Streaming();
        streaming.setId(resultSet.getInt("id"));
        streaming.setAppName(resultSet.getString("app_name"));
        streaming.setAppId(resultSet.getString("app_id"));
        streaming.setState(resultSet.getInt("state"));
        streaming.setLastUpdateTime(resultSet.getTimestamp("last_update_time"));
        streaming.setCreateTime(resultSet.getTimestamp("create_time"));
        streaming.setCreator(resultSet.getString("creator"));
        streaming.setKiller(resultSet.getString("killer"));
      }
      resultSet.close();
      preparedStatement.close();
    } catch (SQLException e) {
      LOGGER.error(
          "select by appName error, streaming name: {}, error message: {}",
          appName,
          ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return streaming;
  }

  /**
   * list all wait operate.
   *
   * @return all waitstart and waitkill
   */
  public List<Streaming> listWait() {
    List<Streaming> list = new ArrayList<>();
    Connection connection = DataBaseUtil.getInstance();
    try {

      PreparedStatement preStmt = connection.prepareStatement(SELECT_ALL_WAIT);
      preStmt.setInt(1, StateEnum.WAITSTART.getNumber());
      preStmt.setInt(2, StateEnum.WAITKILL.getNumber());
      preStmt.setInt(3, StateEnum.WAITRESTART.getNumber());
      ResultSet resultSet = preStmt.executeQuery();
      while (resultSet.next()) {
        Streaming streaming = new Streaming();
        streaming.setId(resultSet.getInt("id"));
        streaming.setAppName(resultSet.getString("app_name"));
        streaming.setAppId(resultSet.getString("app_id"));
        streaming.setState(resultSet.getInt("state"));
        streaming.setLastUpdateTime(resultSet.getTimestamp("last_update_time"));
        streaming.setCreateTime(resultSet.getTimestamp("create_time"));
        streaming.setCreator(resultSet.getString("creator"));
        streaming.setKiller(resultSet.getString("killer"));
        list.add(streaming);
      }
      resultSet.close();
      preStmt.close();
    } catch (SQLException e) {
      LOGGER.error("list wait error, error message: {}", ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return list;
  }

  /**
   * 得到运行中的或者正在启动的app.
   *
   * @return app列表
   */
  public List<Streaming> selectAll() {
    List<Streaming> streamings = new ArrayList<>();
    Connection connection = DataBaseUtil.getInstance();
    try {

      PreparedStatement preStmt = connection.prepareStatement(SELECT_SQL);
      ResultSet rs = preStmt.executeQuery();
      while (rs.next()) {
        Streaming streaming = new Streaming();
        streaming.setId(rs.getInt("id"));
        streaming.setAppName(rs.getString("app_name"));
        streaming.setAppId(rs.getString("app_id"));
        streaming.setState(rs.getInt("state"));
        streaming.setCreateTime(rs.getTimestamp("create_time"));
        streaming.setLastUpdateTime(rs.getTimestamp("last_update_time"));
        streaming.setCreator(rs.getString("creator"));
        streaming.setKiller(rs.getString("killer"));
        streamings.add(streaming);
      }
      rs.close();
      preStmt.close();
    } catch (SQLException e) {
      LOGGER.error("select all error, error message: {}", ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return streamings;
  }

  /** select by state. */
  public List<Streaming> selectByState(List<Integer> states) {
    String sql =
        "SELECT * FROM streaming_appinfo WHERE state in (" + StringUtils.join(states, ", ") + ")";
    List<Streaming> streamings = new ArrayList<>();
    Connection connection = DataBaseUtil.getInstance();
    try {

      PreparedStatement preStmt = connection.prepareStatement(sql);
      ResultSet rs = preStmt.executeQuery();

      while (rs.next()) {
        Streaming streaming = new Streaming();
        streaming.setId(rs.getInt("id"));
        streaming.setAppName(rs.getString("app_name"));
        streaming.setAppId(rs.getString("app_id"));
        streaming.setState(rs.getInt("state"));
        streaming.setCreateTime(rs.getTimestamp("create_time"));
        streaming.setLastUpdateTime(rs.getTimestamp("last_update_time"));
        streaming.setCreator(rs.getString("creator"));
        streaming.setKiller(rs.getString("killer"));
        streamings.add(streaming);
      }
      rs.close();
      preStmt.close();
    } catch (SQLException e) {
      LOGGER.error("select all error, error message: {}", ExceptionUtils.getFullStackTrace(e));
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return streamings;
  }
}
