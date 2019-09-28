package com.weibo.dip.web.common;

import com.weibo.dip.web.model.intelligentwarning.Blacklist;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;


public class BlacklistRowMapper implements RowMapper<Blacklist> {

  @Override
  public Blacklist mapRow(ResultSet rs, int rowNum) throws SQLException {
    Blacklist blacklist = new Blacklist();

    blacklist.setId(rs.getInt("id"));
    blacklist.setBlackName(rs.getString("black_name"));
    blacklist.setUpdateTime(rs.getTimestamp("update_time").getTime());
    blacklist.setBusiness(rs.getString("business"));
    blacklist.setDimensions(rs.getString("dimensions"));
    blacklist.setBeginTime(rs.getString("begin_time"));
    blacklist.setFinishTime(rs.getString("finish_time"));

    return blacklist;
  }
}
