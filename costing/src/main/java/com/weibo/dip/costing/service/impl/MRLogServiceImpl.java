package com.weibo.dip.costing.service.impl;

import com.weibo.dip.costing.bean.MRLog;
import com.weibo.dip.costing.dao.MRLogDao;
import com.weibo.dip.costing.service.MRLogService;
import java.util.Date;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by yurun on 18/4/24.
 */
@Service
public class MRLogServiceImpl implements MRLogService {

  @Autowired
  private MRLogDao mrLogDao;

  @Override
  public List<MRLog> gets(Date beginTime, Date endTime) {
    return mrLogDao.gets(beginTime, endTime);
  }

}
