package com.weibo.dip.web.dao.impl;

import com.weibo.dip.web.common.KafkaConsumerRowMapper;
import com.weibo.dip.web.dao.KafkaConsumerDao;
import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.datamart.KafkaConsumer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Repository;

/**
 * Created by shixi_dongxue3 on 2018/2/6.
 */
@Repository("kafkaConsumerDao")
public class KafkaConsumerDaoImpl implements KafkaConsumerDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerDaoImpl.class);

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Override
  public KafkaConsumer create(KafkaConsumer kafkaConsumer) {
    jdbcTemplate.update(
        "INSERT INTO dataplatform_consumer(id, "
            + "consumer_product, "
            + "topic_name, "
            + "consumer_group, "
            + "contact_person, "
            + "create_time, "
            + "update_time, "
            + "comment) "
            + "VALUES(NULL,?,?,?,?,NULL,NULL,?)",
        new PreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps) throws SQLException {
            ps.setString(1, kafkaConsumer.getConsumerProduct());
            ps.setString(2, kafkaConsumer.getTopicName());
            ps.setString(3, kafkaConsumer.getConsumerGroup());
            ps.setString(4, kafkaConsumer.getContactPerson());
            if (StringUtils.isBlank(kafkaConsumer.getComment())) {
              ps.setString(5, null);
            } else {
              ps.setString(5, kafkaConsumer.getComment());
            }
          }
        });

    return kafkaConsumer;
  }

  @Override
  public void delete(Integer id) {
    String sql = "delete from dataplatform_consumer where id=?";
    jdbcTemplate.update(sql, id);
  }

  @Override
  public void update(KafkaConsumer kafkaConsumer) {
    jdbcTemplate.update(
        "update dataplatform_consumer set "
            + "consumer_product=?, topic_name=?, consumer_group=?, contact_person=?, comment=? "
            + "where id=?",
        new PreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps) throws SQLException {
            ps.setString(1, kafkaConsumer.getConsumerProduct());
            ps.setString(2, kafkaConsumer.getTopicName());
            ps.setString(3, kafkaConsumer.getConsumerGroup());
            ps.setString(4, kafkaConsumer.getContactPerson());
            if (StringUtils.isBlank(kafkaConsumer.getComment())) {
              ps.setString(5, null);
            } else {
              ps.setString(5, kafkaConsumer.getComment());
            }
            ps.setInt(6, kafkaConsumer.getId());
          }
        });
  }

  @Override
  public KafkaConsumer findKafkaConsumerById(Integer id) {
    return jdbcTemplate
        .queryForObject("select * from dataplatform_consumer where id=?", new Object[]{id},
            new KafkaConsumerRowMapper());
  }

  @Override
  public List<KafkaConsumer> searching(Searching searching, int column, String dir) {
    String[] line = {"id", "topic_name", "consumer_group", "consumer_product", "contact_person",
        "create_time", "update_time"};
    List<KafkaConsumer> list = null;
    String sql;
    if (searching.isExit()) {
      sql = "select * from dataplatform_consumer where " + searching.getCondition() + " like '%"
          + searching.getKeyword() + "%' and create_time between '" + searching.getStarttime()
          + "' and '" + searching.getEndtime() + "' order by " + line[column] + " " + dir;

    } else {
      sql = "select * from dataplatform_consumer order by " + line[column] + " " + dir;
    }
    list = jdbcTemplate.query(sql, new KafkaConsumerRowMapper());
    return list;
  }

  @Override
  public Integer isUnique(String topicName, String consumerGroup) {
    String sql = "select count(*) from dataplatform_consumer where topic_name='" + topicName
        + "' and consumer_group='" + consumerGroup + "'";

    return jdbcTemplate.queryForObject(sql, Integer.class);
  }

}
