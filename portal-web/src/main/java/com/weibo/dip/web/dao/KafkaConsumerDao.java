package com.weibo.dip.web.dao;

import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.datamart.KafkaConsumer;

import java.util.List;

/**
 * Created by shixi_dongxue3 on 2018/2/6.
 */
public interface KafkaConsumerDao {

  /**
   * 新增一个consumer.
   */
  KafkaConsumer create(KafkaConsumer kafkaConsumer);

  /**
   * 根据id删除一个consumer.
   */
  void delete(Integer id);

  /**
   * 修改一个consumer.
   */
  void update(KafkaConsumer kafkaConsumer);

  /**
   * 根据id查找一个consumer.
   */
  KafkaConsumer findKafkaConsumerById(Integer id);

  /**
   * 查询并分页展示.
   */
  List<KafkaConsumer> searching(Searching searching, int column, String dir);

  /**
   * 查询topicName和consumerGroup的组合是否已存在 存在返回1，不存在返回0.
   */
  Integer isUnique(String topicName, String consumerGroup);

}
