package com.weibo.dip.web.common;

import com.weibo.dip.web.model.datamart.KafkaTopic;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

/**
 * Created by shixi_dongxue3 on 2018/1/31.
 */
public class KafkaTopicRowMapper implements RowMapper<KafkaTopic> {

  @Override
  public KafkaTopic mapRow(ResultSet rs, int i) throws SQLException {
    KafkaTopic kafkaTopic = new KafkaTopic();

    kafkaTopic.setId(rs.getInt("id"));
    kafkaTopic.setTopicName(rs.getString("topic_name"));
    kafkaTopic.setProduct(rs.getString("product_uuid"));
    rs.getInt("peak_traffic");                            //当字段可以为空时这样写
    if (!rs.wasNull()) {
      kafkaTopic.setPeekTraffic(rs.getInt("peak_traffic"));
    }
    rs.getInt("peak_qps");
    if (!rs.wasNull()) {
      kafkaTopic.setPeekQps(rs.getInt("peak_qps"));
    }
    rs.getInt("datasize_count");
    if (!rs.wasNull()) {
      kafkaTopic.setDatasizeNumber(rs.getInt("datasize_count"));
    }
    rs.getInt("datasize_space");
    if (!rs.wasNull()) {
      kafkaTopic.setDatasizeSpace(rs.getInt("datasize_space"));
    }
    rs.getInt("log_size");
    if (!rs.wasNull()) {
      kafkaTopic.setLogsize(rs.getInt("log_size"));
    }
    kafkaTopic.setCreateTime(rs.getTimestamp("create_time"));
    kafkaTopic.setUpdateTime(rs.getTimestamp("update_time"));
    kafkaTopic.setClusterName(rs.getString("cluster_name"));
    kafkaTopic.setLogDescripton(rs.getString("log_desc"));
    kafkaTopic.setContactPerson(rs.getString("contact_person"));
    rs.getString("comment");
    if (!rs.wasNull()) {
      kafkaTopic.setComment(rs.getString("comment"));
    }

    return kafkaTopic;
  }
}
