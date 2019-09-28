package com.weibo.dip.web.common;

import com.weibo.dip.web.dao.KafkaConsumerDao;
import com.weibo.dip.web.dao.impl.KafkaConsumerDaoImpl;
import com.weibo.dip.web.model.datamart.KafkaConsumer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

/**
 * Created by shixi_dongxue3 on 2018/2/6.
 */
public class KafkaConsumerRowMapper implements RowMapper<KafkaConsumer> {

  KafkaConsumerDao kafkaConsumerDao = new KafkaConsumerDaoImpl();

  @Override
  public KafkaConsumer mapRow(ResultSet rs, int i) throws SQLException {
    KafkaConsumer kafkaConsumer = new KafkaConsumer();

    kafkaConsumer.setId(rs.getInt("id"));
    kafkaConsumer.setConsumerProduct(rs.getString("consumer_product"));
    kafkaConsumer.setTopicName(rs.getString("topic_name"));
    kafkaConsumer.setConsumerGroup(rs.getString("consumer_group"));
    kafkaConsumer.setContactPerson(rs.getString("contact_person"));
    kafkaConsumer.setCreateTime(rs.getTimestamp("create_time"));
    kafkaConsumer.setUpdateTime(rs.getTimestamp("update_time"));
    rs.getString("comment");
    if (!rs.wasNull()) {
      kafkaConsumer.setComment(rs.getString("comment"));
    }

    return kafkaConsumer;
  }
}
