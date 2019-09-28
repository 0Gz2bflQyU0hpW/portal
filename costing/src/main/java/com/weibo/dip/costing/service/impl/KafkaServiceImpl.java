package com.weibo.dip.costing.service.impl;

import com.weibo.dip.costing.bean.KafkaResource;
import com.weibo.dip.costing.dao.KafkaDao;
import com.weibo.dip.costing.service.KafkaService;
import java.util.Date;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by yurun on 18/4/25.
 */
@Service
public class KafkaServiceImpl implements KafkaService {

  @Autowired
  private KafkaDao kafkaDao;

  @Override
  public List<KafkaResource> gets(Date beginTime, Date endTime) {
    return kafkaDao.gets(beginTime, endTime);
  }

}
