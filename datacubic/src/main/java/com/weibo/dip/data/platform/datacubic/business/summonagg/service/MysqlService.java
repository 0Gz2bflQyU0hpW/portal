package com.weibo.dip.data.platform.datacubic.business.summonagg.service;

/**
 * Created by liyang28 on 2018/7/28.
 */
public interface MysqlService {

  void sendMysqlAggStatus(String index, long produceCount);
}
