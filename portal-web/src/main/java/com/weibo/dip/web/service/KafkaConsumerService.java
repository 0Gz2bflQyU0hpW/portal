package com.weibo.dip.web.service;

import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.datamart.KafkaConsumer;

import java.util.List;

/**
 * Created by shixi_dongxue3 on 2018/2/6.
 */
public interface KafkaConsumerService {

  /**
   * 新增一个consumer.
   *
   * @param kafkaConsumer 新的consumer
   * @return 返回新创建的consumer
   */
  KafkaConsumer create(KafkaConsumer kafkaConsumer);

  /**
   * 根据id删除一个consumer.
   *
   * @param id 要删除的id
   */
  void delete(Integer id);

  /**
   * 修改一个consumer.
   *
   * @param kafkaConsumer 更新的consumer
   */
  void update(KafkaConsumer kafkaConsumer);

  /**
   * 根据id查找一个consumer.
   *
   * @param id 要查询的id
   * @return 详细信息
   */
  KafkaConsumer findKafkaConsumerById(Integer id);

  /**
   * 查询符合条件的结果.
   *
   * @param json 包含搜索条件或者为排序条件的json解析
   * @return 所有查询到的consumer
   */
  List<KafkaConsumer> searching(AnalyzeJson json);

  /**
   * 查询topicName和consumerGroup的组合是否已存在 存在返回1，不存在返回0.
   *
   * @param topicName topic名称
   * @param consumerGroup consumer所属分组
   * @return 存在返回1，不存在返回0
   */
  Integer isUnique(String topicName, String consumerGroup);

}

