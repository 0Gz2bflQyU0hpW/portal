package com.weibo.dip.web.common;

import com.weibo.dip.web.model.intelligentwarning.Strategy;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

/**
 * Created by shixi_dongxue3 on 2018/3/12.
 */
public class StrategyRowMapper implements RowMapper<Strategy> {

  @Override
  public Strategy mapRow(ResultSet rs, int rowNum) throws SQLException {
    Strategy strategy = new Strategy();

    strategy.setId(rs.getInt("id"));
    strategy.setStrategyName(rs.getString("strategy_name"));
    strategy.setUpdateTime(rs.getTimestamp("update_time").getTime());
    strategy.setBusiness(rs.getString("business"));
    strategy.setDimensions(rs.getString("dimensions"));
    strategy.setMetrics(rs.getString("metrics"));
    strategy.setContactPerson(rs.getString("contact_person"));
    strategy.setStatus(rs.getString("status"));

    return strategy;
  }
}
