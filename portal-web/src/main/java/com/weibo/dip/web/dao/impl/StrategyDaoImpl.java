package com.weibo.dip.web.dao.impl;

import com.weibo.dip.web.common.StrategyRowMapper;
import com.weibo.dip.web.dao.StrategyDao;
import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.intelligentwarning.Strategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
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
 * Created by shixi_dongxue3 on 2018/3/12.
 */
@Repository("StrategyDao")
public class StrategyDaoImpl implements StrategyDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(StrategyDaoImpl.class);

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Override
  public Map<String, Object> create(Strategy strategy) {
    Map<String, Object> map = new HashMap<>();

    if (isExist(strategy.getStrategyName())) {
      LOGGER.error("the strategyName(" + strategy.getStrategyName() + ") already exists！");
      return null;
    } else {
      jdbcTemplate.update(
          "INSERT INTO warning_strategy(id, "
              + "strategy_name, "
              + "update_time, "
              + "business, "
              + "dimensions, "
              + "metrics, "
              + "contact_person, "
              + "status) "
              + "VALUES(NULL,?,NULL,?,?,?,?,?)",
          new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
              ps.setString(1, strategy.getStrategyName());
              ps.setString(2, strategy.getBusiness());
              ps.setString(3, strategy.getDimensions());
              ps.setString(4, strategy.getMetrics());
              ps.setString(5, strategy.getContactPerson());
              ps.setString(6, strategy.getStatus());
            }
          });

      int id = jdbcTemplate.queryForObject(
          "select id from warning_strategy where strategy_name='" + strategy.getStrategyName()
              + "'", Integer.class);

      map.put("id", id);
      map.put("time", getTime(strategy.getStrategyName()));
      return map;
    }
  }

  @Override
  public boolean delete(int id) {
    String sql = "delete from warning_strategy where id=?";
    int resule = jdbcTemplate.update(sql, id);

    if (resule == 1) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public long update(Strategy strategy) {
    jdbcTemplate.update(
        "update warning_strategy set "
            + "strategy_name =?, business=?, dimensions=?, metrics=?, contact_person=?, status=? "
            + "where id=?",
        new PreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps) throws SQLException {
            ps.setString(1, strategy.getStrategyName());
            ps.setString(2, strategy.getBusiness());
            ps.setString(3, strategy.getDimensions());
            ps.setString(4, strategy.getMetrics());
            ps.setString(5, strategy.getContactPerson());
            ps.setString(6, strategy.getStatus());
            ps.setInt(7, strategy.getId());
          }
        });

    return getTime(strategy.getStrategyName());
  }

  @Override
  public long updateStatus(String status, int id) {
    jdbcTemplate
        .update("update warning_strategy set status=? where id=?", new PreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps) throws SQLException {
            ps.setString(1, status);
            ps.setInt(2, id);
          }
        });

    return findStrategyById(id).getUpdateTime();
  }

  @Override
  public Strategy findStrategyById(int id) {
    return jdbcTemplate
        .queryForObject("select * from warning_strategy where id=?", new Object[] {id},
            new StrategyRowMapper());
  }

  @Override
  public List<Strategy> searching(Searching searching, int column, String dir) {
    String[] line = {"id", "strategy_name", "business", "dimensions", "contact_person", "status"};
    String sql;

    if (searching.isExit()) {
      sql = "select * from warning_strategy where " + searching.getCondition() + " like '%"
          + searching.getKeyword() + "%' and update_time between '" + searching.getStarttime()
          + "' and '" + searching.getEndtime() + "' order by " + line[column] + " " + dir;
    } else {
      sql = "select * from warning_strategy order by " + line[column] + " " + dir;
    }
    List<Strategy> strategies = jdbcTemplate.query(sql, new StrategyRowMapper());
    return strategies;
  }

  @Override
  public boolean isExist(String strategyName) {
    String sql = "select count(*) from warning_strategy where strategy_name='" + strategyName + "'";
    int resule = jdbcTemplate.queryForObject(sql, Integer.class);

    if (resule == 1) {
      return true;
    } else {
      return false;
    }
  }

  //根据strategy_name获取update_time
  private long getTime(String strategyName) {
    String sql =
        "select update_time from warning_strategy where strategy_name='" + strategyName + "'";
    Timestamp time = jdbcTemplate.queryForObject(sql, Timestamp.class);

    return time.getTime();
  }
}
