package com.weibo.dip.data.platform.datacubic.business.summonagg.service.impl;

import com.weibo.dip.data.platform.datacubic.business.summonagg.dao.AggStatus;
import com.weibo.dip.data.platform.datacubic.business.summonagg.service.MysqlService;
import scala.Serializable;

/**
 * Created by liyang28 on 2018/7/28.
 */
public class MysqlServiceImpl implements MysqlService, Serializable {

  public MysqlServiceImpl() {
  }

  @Override
  public void sendMysqlAggStatus(String index, long produceCount) {
    AggStatus.sendMysqlStatus(index, produceCount);
  }
}
