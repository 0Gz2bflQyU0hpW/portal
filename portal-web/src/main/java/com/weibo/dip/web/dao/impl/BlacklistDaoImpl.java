package com.weibo.dip.web.dao.impl;

import com.weibo.dip.web.common.BlacklistRowMapper;
import com.weibo.dip.web.dao.BlacklistDao;
import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.intelligentwarning.Blacklist;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Repository;


/**
 * Created by shixi_ruibo on 2018/3/13.
 */
@Repository("BlacklistDao")
public class BlacklistDaoImpl implements BlacklistDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlacklistDaoImpl.class);

  @Autowired
  private JdbcTemplate jdbcTemplate;


  @Override
  public Map<String, Object> create(Blacklist blacklist) {
    Map<String, Object> map = new HashMap<>();
    if (isExist(blacklist.getBlackName())) {
      LOGGER.error("the blackName(" + blacklist.getBlackName() + ") already exists！");
      return null;
    } else {
      jdbcTemplate.update(
          "INSERT INTO warning_blacklist"
              + "(id, black_name, update_time, business, dimensions, begin_time,finish_time) "
              + "VALUES(NULL,?,NULL,?,?,?,?)",
          new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
              ps.setString(1, blacklist.getBlackName());
              ps.setString(2, blacklist.getBusiness());
              ps.setString(3, blacklist.getDimensions());
              String st1 = blacklist.getBeginTime();
              DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
              Date st = null;
              try {
                st = fmt.parse(st1);
              } catch (ParseException e) {
                e.printStackTrace();
              }
              String et1 = blacklist.getFinishTime();

              Date et = null;
              try {
                et = fmt.parse(et1);
              } catch (ParseException e) {
                e.printStackTrace();
              }
              ps.setTimestamp(4, new java.sql.Timestamp(st.getTime()));
              ps.setTimestamp(5, new java.sql.Timestamp(et.getTime()));
            }

          });
      int id = jdbcTemplate.queryForObject(
          "select id from warning_blacklist where black_name='" + blacklist.getBlackName() + "'",
          Integer.class);

      map.put("id", id);
      map.put("time", getTime(blacklist.getBlackName()));
      return map;
    }
  }

  @Override
  public boolean delete(int id) {
    String sql = "delete from warning_blacklist where id=?";
    int resule = jdbcTemplate.update(sql, id);

    if (resule == 1) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public long update(Blacklist blacklist) {

    jdbcTemplate.update(
        "update warning_blacklist set "
            + "black_name =?, business=?, dimensions=?, begin_time=?, finish_time=? "
            + "where id=?",
        new PreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps) throws SQLException {
            ps.setString(1, blacklist.getBlackName());
            ps.setString(2, blacklist.getBusiness());
            ps.setString(3, blacklist.getDimensions());
            ps.setString(4, blacklist.getBeginTime());
            ps.setString(5, blacklist.getFinishTime());
            ps.setInt(6, blacklist.getId());
          }
        });
    return getTime(blacklist.getBlackName());
  }

  @Override
  public Blacklist findBlacklistById(int id) {
    return jdbcTemplate
        .queryForObject("select * from warning_blacklist where id=?", new Object[]{id},
            new BlacklistRowMapper());
  }

  @Override
  public List<Blacklist> searching(Searching searching, int column, String dir) {
    String[] line = {"id", "black_name", "business", "dimensions", "begin_time", "finish_time"};
    String sql;

    if (searching.isExit()) {
      sql = "select * from warning_blacklist where " + searching.getCondition() + " like '%"
          + searching.getKeyword() + "%' and update_time between '" + searching.getStarttime()
          + "' and '" + searching.getEndtime() + "' order by " + line[column] + " " + dir;
    } else {
      sql = "select * from warning_blacklist order by " + line[column] + " " + dir;
    }
    List<Blacklist> blacklistes = jdbcTemplate.query(sql, new BlacklistRowMapper());
    return blacklistes;
  }

  @Override
  public boolean isExist(String blackName) {
    String sql = "select count(*) from warning_blacklist where black_name='" + blackName + "'";
    int resule = jdbcTemplate.queryForObject(sql, Integer.class);

    if (resule == 1) {
      return true;
    } else {
      return false;
    }
  }

  private long getTime(String blackName) {
    String sql = "select update_time from warning_blacklist where black_name='" + blackName + "'";
    Timestamp time = jdbcTemplate.queryForObject(sql, Timestamp.class);

    return time.getTime();
  }
}
