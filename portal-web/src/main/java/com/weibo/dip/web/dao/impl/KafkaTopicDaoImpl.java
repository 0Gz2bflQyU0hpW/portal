package com.weibo.dip.web.dao.impl;

import com.weibo.dip.web.common.KafkaTopicRowMapper;
import com.weibo.dip.web.dao.KafkaTopicDao;
import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.datamart.KafkaTopic;

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
 * Created by shixi_dongxue3 on 2018/1/31.
 */
@Repository("kafkaTopicDao")
public class KafkaTopicDaoImpl implements KafkaTopicDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicDaoImpl.class);

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Override
  public KafkaTopic create(KafkaTopic kafkaTopic) {
    jdbcTemplate.update(
        "INSERT INTO dataplatform_topic_v2(id, "
            + "topic_name, "
            + "product_uuid, "
            + "peak_traffic, "
            + "peak_qps, "
            + "datasize_count, "
            + "datasize_space, "
            + "log_size, "
            + "contact_person, "
            + "create_time, "
            + "update_time, "
            + "cluster_name, "
            + "log_desc, "
            + "comment) "
            + "VALUES(NULL,?,?,?,?,?,?,?,?,NULL,NULL,?,?,?)",
        new PreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps) throws SQLException {
            ps.setString(1, kafkaTopic.getTopicName());
            ps.setString(2, kafkaTopic.getProduct());
            if (kafkaTopic.getPeekTraffic() == null) {
              ps.setString(3, null);
            } else {
              ps.setInt(3, kafkaTopic.getPeekTraffic());
            }
            if (kafkaTopic.getPeekQps() == null) {
              ps.setString(4, null);
            } else {
              ps.setInt(4, kafkaTopic.getPeekQps());
            }
            if (kafkaTopic.getDatasizeNumber() == null) {
              ps.setString(5, null);
            } else {
              ps.setInt(5, kafkaTopic.getDatasizeNumber());
            }
            if (kafkaTopic.getDatasizeSpace() == null) {
              ps.setString(6, null);
            } else {
              ps.setInt(6, kafkaTopic.getDatasizeSpace());
            }
            if (kafkaTopic.getLogsize() == null) {
              ps.setString(7, null);
            } else {
              ps.setInt(7, kafkaTopic.getLogsize());
            }
            ps.setString(8, kafkaTopic.getContactPerson());
            if (StringUtils.isBlank(kafkaTopic.getClusterName())) {
              ps.setString(9, null);
            } else {
              ps.setString(9, kafkaTopic.getClusterName());
            }
            if (StringUtils.isBlank(kafkaTopic.getLogDescripton())) {
              ps.setString(10, null);
            } else {
              ps.setString(10, kafkaTopic.getLogDescripton());
            }
            if (StringUtils.isBlank(kafkaTopic.getComment())) {
              ps.setString(11, null);
            } else {
              ps.setString(11, kafkaTopic.getComment());
            }
          }
        });

    return kafkaTopic;
  }

  @Override
  public void delete(Integer id) {
    String sql = "delete from dataplatform_topic_v2 where id=?";
    jdbcTemplate.update(sql, id);
  }

  @Override
  public void update(KafkaTopic kafkaTopic) {
    jdbcTemplate.update(
        "update dataplatform_topic_v2 set "
            + "topic_name =?, "
            + "product_uuid=?, "
            + "peak_traffic=?, "
            + "peak_qps=?, "
            + "datasize_count=?, "
            + "datasize_space=?, "
            + "log_size=?, "
            + "contact_person=?,"
            + "cluster_name=?, "
            + "log_desc=?,"
            + "comment=? "
            + "where id=?",
        new PreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps) throws SQLException {
            ps.setString(1, kafkaTopic.getTopicName());
            ps.setString(2, kafkaTopic.getProduct());
            if (kafkaTopic.getPeekTraffic() == null) {
              ps.setString(3, null);
            } else {
              ps.setInt(3, kafkaTopic.getPeekTraffic());
            }
            if (kafkaTopic.getPeekQps() == null) {
              ps.setString(4, null);
            } else {
              ps.setInt(4, kafkaTopic.getPeekQps());
            }
            if (kafkaTopic.getDatasizeNumber() == null) {
              ps.setString(5, null);
            } else {
              ps.setInt(5, kafkaTopic.getDatasizeNumber());
            }
            if (kafkaTopic.getDatasizeSpace() == null) {
              ps.setString(6, null);
            } else {
              ps.setInt(6, kafkaTopic.getDatasizeSpace());
            }
            if (kafkaTopic.getLogsize() == null) {
              ps.setString(7, null);
            } else {
              ps.setInt(7, kafkaTopic.getLogsize());
            }
            ps.setString(8, kafkaTopic.getContactPerson());
            if (StringUtils.isBlank(kafkaTopic.getClusterName())) {
              ps.setString(9, null);
            } else {
              ps.setString(9, kafkaTopic.getClusterName());
            }
            if (StringUtils.isBlank(kafkaTopic.getLogDescripton())) {
              ps.setString(10, null);
            } else {
              ps.setString(10, kafkaTopic.getLogDescripton());
            }
            if (StringUtils.isBlank(kafkaTopic.getComment())) {
              ps.setString(11, null);
            } else {
              ps.setString(11, kafkaTopic.getComment());
            }
            ps.setInt(12, kafkaTopic.getId());
          }
        });
  }

  @Override
  public KafkaTopic findKafkaTopicById(Integer id) {
    return jdbcTemplate
        .queryForObject("select * from dataplatform_topic_v2 where id=?", new Object[]{id},
            new KafkaTopicRowMapper());
  }

  @Override
  public List<KafkaTopic> searching(Searching searching, int column, String dir) {
    String[] line = {"id", "topic_name", "product_uuid", "peak_traffic", "peak_qps",
        "datasize_count", "datasize_space", "log_size", "cluster_name"};
    List<KafkaTopic> list = null;
    String sql;
    if (searching.isExit()) {
      sql = "select * from dataplatform_topic_v2 where " + searching.getCondition() + " like '%"
          + searching.getKeyword() + "%' and create_time between '" + searching.getStarttime()
          + "' and '" + searching.getEndtime() + "' order by " + line[column] + " " + dir;

    } else {
      sql = "select * from dataplatform_topic_v2 order by " + line[column] + " " + dir;
    }
    list = jdbcTemplate.query(sql, new KafkaTopicRowMapper());
    return list;

  }

  @Override
  public Integer isUnique(String topicName) {
    String sql = "select count(*) from dataplatform_topic_v2 where topic_name='" + topicName + "'";

    return jdbcTemplate.queryForObject(sql, Integer.class);
  }
}
