package com.weibo.dip.web.dao;

import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.datamart.KafkaTopic;

import java.util.List;

/**
 * Created by shixi_dongxue3 on 2018/1/31.
 */
public interface KafkaTopicDao {

  /**
   * 新增一个topic.
   */
  KafkaTopic create(KafkaTopic kafkaTopic);

  /**
   * 根据id删除一个topic.
   */
  void delete(Integer id);

  /**
   * 修改一个topic.
   */
  void update(KafkaTopic kafkaTopic);

  /**
   * 根据id查找一个topic.
   */
  KafkaTopic findKafkaTopicById(Integer id);

  /**
   * 查询并分页展示.
   */
  List<KafkaTopic> searching(Searching searching, int column, String dir);

  /**
   * 查询topicName是否已存在 存在返回1，不存在返回0.
   */
  Integer isUnique(String topicName);
}
