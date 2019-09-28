package com.weibo.dip.costing.dao.impl;

import com.weibo.dip.costing.bean.KafkaResource;
import com.weibo.dip.costing.dao.KafkaDao;
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
public class KafkaDaoImpl implements KafkaDao {

  @Autowired
  private JdbcTemplate primaryJT;

  private static class KafkaResourceMapper implements RowMapper<KafkaResource> {

    @Override
    public KafkaResource mapRow(ResultSet rs, int rowNum) throws SQLException {
      KafkaResource resource = new KafkaResource();

      resource.setId(rs.getInt("id"));
      resource.setTopicName(rs.getString("topic_name"));
      resource.setProductUuid(rs.getString("product_uuid"));
      resource.setPeakBytesout(rs.getDouble("peak_bytes_out"));
      resource.setComment(rs.getString("comment"));
      resource.setCollectTime(new Date(rs.getTimestamp("collect_time").getTime()));

      return resource;
    }

  }

  @Override
  public List<KafkaResource> gets(Date begintTime, Date endTime) {
    String sql = "select * from kafka_resource_statistics where collect_time >= ? and collect_time <= ?";

    return primaryJT.query(sql, new KafkaResourceMapper(), begintTime, endTime);
  }

}
