package com.weibo.dip.costing.dao.impl;

import com.weibo.dip.costing.bean.StreamingResource;
import com.weibo.dip.costing.dao.StreamingDao;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * Created by yurun on 18/4/25.
 */
@Repository
public class StreamingDaoImpl implements StreamingDao {

  @Autowired
  private JdbcTemplate primaryJT;

  private static class StreamingResourceMapper implements RowMapper<StreamingResource> {

    @Override
    public StreamingResource mapRow(ResultSet rs, int rowNum) throws SQLException {
      StreamingResource resource = new StreamingResource();

      resource.setId(rs.getInt("id"));
      resource.setAppName(rs.getString("appname"));
      resource.setProductUuid(rs.getString("product_uuid"));
      resource.setCores(rs.getInt("cores"));
      resource.setMemories(rs.getInt("memories"));
      resource.setServer(rs.getInt("servers"));
      resource.setCollectTimestamp(new Date(rs.getTimestamp("collect_timestamp").getTime()));
      return resource;
    }

  }

  @Override
  public List<StreamingResource> gets(Date begintTime, Date endTime) {
    String sql = "select * from streaming_resource_statistics where collect_timestamp >= ? and collect_timestamp <= ?";

    return primaryJT.query(sql, new StreamingResourceMapper(), begintTime, endTime);
  }

}
