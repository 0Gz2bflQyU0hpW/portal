package com.weibo.dip.web.service.impl;

import com.weibo.dip.web.dao.KafkaTopicDao;
import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.datamart.KafkaTopic;
import com.weibo.dip.web.service.KafkaTopicService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by shixi_dongxue3 on 2018/1/31.
 */

@Service
public class KafkaTopicServiceImpl implements KafkaTopicService {

  @Autowired
  private KafkaTopicDao kafkaTopicDao;

  @Override
  public KafkaTopic create(KafkaTopic kafkaTopic) {
    return this.kafkaTopicDao.create(kafkaTopic);
  }

  @Override
  public void delete(Integer id) {
    this.kafkaTopicDao.delete(id);
  }

  @Override
  public void update(KafkaTopic kafkaTopic) {
    this.kafkaTopicDao.update(kafkaTopic);
  }

  @Override
  public KafkaTopic findKafkaTopicById(Integer id) {
    return this.kafkaTopicDao.findKafkaTopicById(id);
  }

  @Override
  public List<KafkaTopic> searching(AnalyzeJson json) {
    Searching searching = new Searching(json.getCondition(), json.getStarttime(), json.getEndtime(),
        json.getKeyword());
    List<KafkaTopic> list = kafkaTopicDao.searching(searching, json.getColumn(), json.getDir());
    return list;
  }

  @Override
  public Integer isUnique(String topicName) {
    return this.kafkaTopicDao.isUnique(topicName);
  }

}
