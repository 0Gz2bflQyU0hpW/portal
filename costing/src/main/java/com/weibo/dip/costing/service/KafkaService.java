package com.weibo.dip.costing.service;

import com.weibo.dip.costing.bean.KafkaResource;
import java.util.Date;
import java.util.List;

/**
 * Created by yurun on 18/4/25.
 */
public interface KafkaService {

  List<KafkaResource> gets(Date beginTime, Date endTime);

}
