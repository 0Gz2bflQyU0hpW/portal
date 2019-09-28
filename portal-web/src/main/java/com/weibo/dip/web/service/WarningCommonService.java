package com.weibo.dip.web.service;

import java.util.List;
import java.util.Set;

/** Created by haisen on 2018/5/21. */
public interface WarningCommonService {
  /*得到所有的business.*/
  List<String> getAllBusiness();

  /*得到某个指定business的维度*/
  Set<String> getDimensionsBybusiness(String business);

  /*得到某个指定business的策略指标*/
  Set<String> getMetricsBybusiness(String business);
}
