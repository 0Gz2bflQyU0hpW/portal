package com.weibo.dip.costing.service.impl;

import com.weibo.dip.costing.bean.SummonResource;
import com.weibo.dip.costing.dao.SummonDao;
import com.weibo.dip.costing.service.SummonService;
import java.util.Date;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by yurun on 18/4/25.
 */
@Service
public class SummonServiceImpl implements SummonService {

  @Autowired
  private SummonDao summonDao;

  @Override
  public List<SummonResource> gets(Date beginTime, Date endTime) {
    return summonDao.gets(beginTime, endTime);
  }

}
