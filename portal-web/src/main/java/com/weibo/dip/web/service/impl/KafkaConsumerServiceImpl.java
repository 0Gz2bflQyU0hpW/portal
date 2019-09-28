package com.weibo.dip.web.service.impl;

import com.weibo.dip.web.dao.KafkaConsumerDao;
import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.datamart.KafkaConsumer;
import com.weibo.dip.web.service.KafkaConsumerService;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by shixi_dongxue3 on 2018/2/6.
 */
@Service
public class KafkaConsumerServiceImpl implements KafkaConsumerService {

  @Autowired
  private KafkaConsumerDao kafkaConsumerDao;

  @Override
  public KafkaConsumer create(KafkaConsumer kafkaconsumer) {
    return this.kafkaConsumerDao.create(kafkaconsumer);
  }

  @Override
  public void delete(Integer id) {
    this.kafkaConsumerDao.delete(id);
  }

  @Override
  public void update(KafkaConsumer kafkaconsumer) {
    this.kafkaConsumerDao.update(kafkaconsumer);
  }

  @Override
  public KafkaConsumer findKafkaConsumerById(Integer id) {
    return this.kafkaConsumerDao.findKafkaConsumerById(id);
  }

  @Override
  public List<KafkaConsumer> searching(AnalyzeJson json) {
    Searching searching = new Searching(json.getCondition(), json.getStarttime(), json.getEndtime(),
        json.getKeyword());
    List<KafkaConsumer> list = kafkaConsumerDao
        .searching(searching, json.getColumn(), json.getDir());
    return list;
  }

  @Override
  public Integer isUnique(String topicName, String consumerGroup) {
    return this.kafkaConsumerDao.isUnique(topicName, consumerGroup);
  }

}
