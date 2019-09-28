package com.weibo.dip.web.service;

import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.datamart.KafkaTopic;

import java.util.List;

/**
 * Created by shixi_dongxue3 on 2018/1/31.
 */
public interface KafkaTopicService {

  /**
   * 新增一个topic.
   *
   * @param kafkaTopic 新建的topuic
   * @return 返回新建的topic
   */
  KafkaTopic create(KafkaTopic kafkaTopic);

  /**
   * 根据id删除一个topic.
   *
   * @param id 要删除的is
   */
  void delete(Integer id);

  /**
   * 修改一个topic.
   *
   * @param kafkaTopic 更新的topic
   */
  void update(KafkaTopic kafkaTopic);

  /**
   * 根据id查找一个topic.
   *
   * @param id 要查询的id
   * @return 返回详细信息
   */
  KafkaTopic findKafkaTopicById(Integer id);

  /**
   * 查询符合条件的结果.
   *
   * @param json 包含搜索条件或者为排序条件的json解析
   * @return 所有查询到的topic
   */
  List<KafkaTopic> searching(AnalyzeJson json);

  /**
   * 查询topicName是否已存在 存在返回1，不存在返回0.
   *
   * @param topicName topic名称
   * @return 存在返回1，不存在返回0
   */
  Integer isUnique(String topicName);

}
