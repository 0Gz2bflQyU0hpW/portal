package com.weibo.dip.costing.dao.impl;

import com.weibo.dip.costing.bean.SummonResource;
import com.weibo.dip.costing.dao.SummonDao;
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
public class SummonDaoImpl implements SummonDao {

  @Autowired
  private JdbcTemplate primaryJT;

  private static class SummonResourceMapper implements RowMapper<SummonResource> {

    @Override
    public SummonResource mapRow(ResultSet rs, int rowNum) throws SQLException {
      SummonResource summonResource = new SummonResource();

      summonResource.setId(rs.getInt("id"));
      summonResource.setBusinessName(rs.getString("business_name"));
      summonResource.setProductUuid(rs.getString("product_uuid"));
      summonResource.setDisk(rs.getInt("disk"));
      summonResource.setIndexQps(rs.getInt("index_qps"));
      summonResource.setCollectedTime(new Date(rs.getTimestamp("collected_time").getTime()));

      return summonResource;
    }

  }

  @Override
  public List<SummonResource> gets(Date beginTime, Date endTime) {
    String sql = "select * from summon_resource_statistics where collected_time >= ? and collected_time <= ?";

    return primaryJT.query(sql, new SummonResourceMapper(), beginTime, endTime);
  }

}
